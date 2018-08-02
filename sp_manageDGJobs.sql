CREATE PROCEDURE [dbo].[sp_manageDGJobs] 
	(
		 @agentJobId UNIQUEIDENTIFIER 
		,@action     VARCHAR(50)             = 'EXECUTE'
		,@status     BIT              OUTPUT 
		,@message    VARCHAR(1000)    OUTPUT
		,@SQL        VARCHAR(4000)    OUTPUT
	)
/*
	Developed by: Mauricio Rivera
	Date: 2 Aug 2018
	
	MODIFICATIONS
		USER	DATE		JIRA	DESCRIPTION
		MR		02/08/2018	BI-508	SP to Execute Jobs in Data Governor

*/
AS
BEGIN
	SET @message = '';
	SET @SQL     = '';
	
	DECLARE
	--PROCESS FLOW VARIABLES
		 @continue                   BIT           = 1
		,@sqlScript                  NVARCHAR(MAX) = N''
		,@checkEveryInSeconds        INT           = 1
		,@timeFrequency              VARCHAR(10)   = ''
		,@dependencyStatus           VARCHAR(50)   = ''
		,@DG_serverName              VARCHAR(128)  = ''
		,@DG_databaseName            VARCHAR(128)  = ''
	--FLAGS VARIABLES
		,@forceStopDGJob             BIT           = 0
	--GENERAL VARIABLES
		,@jobStatus                  VARCHAR(50)   = ''
		,@DG_jobStatus               VARCHAR(50)   = ''
		,@agentJobStatus             VARCHAR(50)   = ''
		,@DG_processExecutionId      INT           = 0
		,@DG_processGroupExecutionId INT           = 0;
		
	SET @timeFrequency   = RIGHT('0' + CAST(@checkEveryInSeconds / 3600 AS VARCHAR),2) + ':' + RIGHT('0' + CAST((@checkEveryInSeconds / 60) % 60 AS VARCHAR),2) + ':' + RIGHT('0' + CAST(@checkEveryInSeconds % 60 AS VARCHAR),2);
	SET @DG_serverName   = dbo.udf_getBIConfigParameter('DATAGOVERNOR-DETAILS',1);
	SET @DG_databaseName = dbo.udf_getBIConfigParameter('DATAGOVERNOR-DETAILS',2);
	
	--VALIDATING INPUT VARIABLES
		IF(
			NOT EXISTS(
				SELECT 1
				FROM   dbo.DGobjects
				WHERE  agentJobId = @agentJobId
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error - The Agent Job Id does not exist';
			END
		ELSE IF(UPPER(@action) NOT IN ('EXECUTE','STOP','STATUS'))
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error - The Input Variable @action only allows EXECUTE, STOP and STATUS as a value';
			END
	
	--PROCESSING
		IF(@continue = 1)
			BEGIN
				SET @DG_jobStatus = (
					SELECT a.ExecutionStatus
					FROM   dbo.vw_DGJobsByStatus a
					WHERE  a.agentJobID = @agentJobId
				);
				SET @agentJobStatus = (
					SELECT a.status
					FROM dbo.vw_agentJobStatus a
					WHERE a.jobId = @agentJobId
				);
			END
		
		IF(@continue = 1 AND @action = 'STATUS') --WHEN ACTION IS STATUS
			BEGIN
				IF(@agentJobStatus = 'CANCELED')
					BEGIN
						IF(@DG_jobStatus = 'EXECUTING')
							BEGIN
								SET @continue       = 0;
								SET @message        = 'FAILED';
								SET @forceStopDGJob = 1;
							END
						ELSE
							BEGIN
								SET @continue = 0;
								SET @message  = 'FAILED';
							END
					END
				ELSE
					BEGIN
						SET @message  = @agentJobStatus;
					END
			END	
		ELSE IF(@action = 'EXECUTE' AND @continue = 1) --WHEN ACTION IS EXECUTE
			BEGIN
				IF(UPPER(@DG_jobStatus) = 'EXECUTING')
					BEGIN
						SET @message = UPPER(@DG_jobStatus)
					END
				ELSE
					BEGIN
					--CHEKING IF ANY JOB DEPENDENCY HAS AN EXECUTION ERROR
						IF(
							EXISTS(
								SELECT  1
								FROM    dbo.vw_DGJobsByStatus a
								WHERE 
									    a.agentJobID = @agentJobId
									AND a.okToProceed LIKE 'ERROR - %'
							)
						)
							BEGIN
								SET @continue = 0;
								SET @message  = (
									SELECT  a.okToProceed
									FROM    dbo.vw_DGJobsByStatus a
									WHERE 
										    a.agentJobID  = @agentJobId
										AND a.okToProceed LIKE 'ERROR - %'
									);
							END
						ELSE
							BEGIN
								WHILE (@dependencyStatus != 'PROCEED')
									BEGIN
										SET @dependencyStatus = (
											SELECT  a.okToProceed
											FROM    dbo.vw_DGJobsByStatus a
											WHERE 
												    a.agentJobID  = @agentJobId
										);
										WAITFOR DELAY @timeFrequency;
									END
								
								IF(@dependencyStatus = 'PROCEED')
									BEGIN
										SET @sqlScript = 'EXEC ' + @DG_serverName + '.' + @DG_databaseName + '.dbo.procETLSQLJobExecute ''' + CONVERT(VARCHAR(100),@agentJobId) + '''';
										BEGIN TRY
											EXEC(@sqlScript);
											SET @message = 'EXECUTING';
										END TRY
										BEGIN CATCH
											SET @continue = 0;
											SET @message  = 'SQL Error-01: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											SET @SQL      = @sqlScript;
										END CATCH
									END
							END
					END
			END
		ELSE IF(@action = 'STOP' AND @continue = 1) --WHEN ACTION IS STOP
			BEGIN
				IF(@DG_jobStatus = 'EXECUTING')
					BEGIN
						SET @sqlScript = 'EXEC ' + @DG_serverName + '.' + @DG_databaseName + '.dbo.procETLSQLJobCancel ''' + CONVERT(VARCHAR(100),@agentJobId) + '''';
						BEGIN TRY
							EXEC(@sqlScript);
							
							SET @checkEveryInSeconds = 5;
							SET @timeFrequency = RIGHT('0' + CAST(@checkEveryInSeconds / 3600 AS VARCHAR),2) + ':' + RIGHT('0' + CAST((@checkEveryInSeconds / 60) % 60 AS VARCHAR),2) + ':' + RIGHT('0' + CAST(@checkEveryInSeconds % 60 AS VARCHAR),2);
							WAITFOR DELAY @timeFrequency;
							
							SET @agentJobStatus = (
								SELECT a.status
								FROM dbo.vw_agentJobStatus a
								WHERE a.jobId = @agentJobId
							);
							
							IF(@agentJobStatus = 'EXECUTING')
								BEGIN
									SET @sqlScript = 'EXEC ' + @DG_serverName + '.msdb.dbo.sp_stop_job '''',''' + CONVERT(VARCHAR(100),@agentJobId) + '''';
									BEGIN TRY
										EXEC(@sqlScript);
										
										SET @checkEveryInSeconds = 5;
										SET @timeFrequency = RIGHT('0' + CAST(@checkEveryInSeconds / 3600 AS VARCHAR),2) + ':' + RIGHT('0' + CAST((@checkEveryInSeconds / 60) % 60 AS VARCHAR),2) + ':' + RIGHT('0' + CAST(@checkEveryInSeconds % 60 AS VARCHAR),2);
										WAITFOR DELAY @timeFrequency;
										
										SET @agentJobStatus = (
											SELECT a.status
											FROM dbo.vw_agentJobStatus a
											WHERE a.jobId = @agentJobId
										);
										
										IF(@agentJobStatus = 'EXECUTING')
											BEGIN
												SET @continue = 0;
												SET @message  = 'Error-04: The process tried to stop the Job two times with an unsuccessful result'
											END
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										SET @message  = 'SQL Error-03: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
										SET @SQL      = @sqlScript;
									END CATCH
								END
							
							SET @forceStopDGJob = 1;
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'SQL Error-02: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
							SET @SQL      = @sqlScript;
						END CATCH
					END
					
				SET @DG_jobStatus = (
					SELECT a.ExecutionStatus
					FROM   dbo.vw_DGJobsByStatus a
					WHERE  a.agentJobID = @agentJobId
				);
				
				SET @agentJobStatus = (
					SELECT a.status
					FROM dbo.vw_agentJobStatus a
					WHERE a.jobId = @agentJobId
				);
				
				IF(@agentJobStatus = 'EXECUTING' AND @DG_jobStatus = 'EXECUTING')
					BEGIN
						SET @continue = 0;
						SET @message = 'Error-09: For an unexpected reason, it is not possible to stop the Job';
					END
				ELSE IF(@agentJobStatus = 'EXECUTING' AND @DG_jobStatus = 'FAILED')
					BEGIN
						SET @continue = 0;
						SET @message = 'Error-10: For an unexpected reason, the job was stopped in Data Governor but not in the Agent';
					END
				ELSE
					BEGIN
						SET @message = @agentJobStatus;
					END
			END
			
		IF(@forceStopDGJob = 1)
			BEGIN
				SET @DG_jobStatus = (
					SELECT a.ExecutionStatus
					FROM   dbo.vw_DGJobsByStatus a
					WHERE  a.agentJobID = @agentJobId
				);
				
				SET @agentJobStatus = (
					SELECT a.status
					FROM dbo.vw_agentJobStatus a
					WHERE a.jobId = @agentJobId
				);
							
				IF(@continue = 1 AND @agentJobStatus = 'CANCELED' AND @DG_jobStatus = 'EXECUTING')
					BEGIN
					--DATA GOVERNOR HAS A WRONG JOB STATUS, PROCEED TO UPDATE IT
						BEGIN TRY
							--GETTING THE DG PROCESS EXECUTION ID & PROCESS GROUP EXECUTION ID
								SET @DG_processExecutionId = (
									SELECT DISTINCT a.ProcessExecutionID
									FROM dbo.vw_DGLastExecutionIdByJobId a
									WHERE a.AgentJobID = @agentJobId
								);
								
								SET @DG_processGroupExecutionId = (
									SELECT DISTINCT a.ProcessGroupExecutionID
									FROM dbo.vw_DGLastExecutionIdByJobId a
									WHERE a.AgentJobID = @agentJobId
								);
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'SQL Error-05: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
							SET @SQL      = '';
						END CATCH
						
						BEGIN TRANSACTION;
						
						--UPDATING DG DATA
							IF(@continue = 1)
								BEGIN
									BEGIN TRY
										SET @sqlScript = 'UPDATE ' + @DG_serverName + '.' + @DG_databaseName + '.dbo.ProcessGroupExecution SET EndTime = GETDATE(), Status = ''Failure'' WHERE ProcessGroupExecutionID = ' + CONVERT(VARCHAR(10),@DG_processGroupExecutionId);
										EXEC(@sqlScript);
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										SET @message  = 'SQL Error-06: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
										SET @SQL      = @sqlScript;
									END CATCH
								END
							
							IF(@continue = 1)
								BEGIN
									BEGIN TRY
										SET @sqlScript = 'UPDATE ' + @DG_serverName + '.' + @DG_databaseName + '.dbo.ProcessExecution SET EndTime = GETDATE(), Status = ''Failure'' WHERE ProcessExecutionID = ' + CONVERT(VARCHAR(10),@DG_processExecutionId);
										EXEC(@sqlScript);
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										SET @message  = 'SQL Error-07: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
										SET @SQL      = @sqlScript;
									END CATCH
								END
							
							IF(@continue = 1)
								BEGIN
									BEGIN TRY
										SET @sqlScript = 'INSERT INTO ' + @DG_serverName + '.' + @DG_databaseName + '.dbo.AppLog 
											(SourceApplication,ApplicationVersion,ProcessExecutionID,SourceComponent,LogDateTime,LogEvent,LogMessage,Computer,Operator)
										VALUES
											(
												(SELECT DISTINCT SourceApplication FROM ' + @DG_serverName + '.' + @DG_databaseName + '.dbo.AppLog WHERE ProcessExecutionID = ' + CONVERT(VARCHAR(10),@DG_processExecutionId) + '),
												(SELECT DISTINCT ApplicationVersion FROM ' + @DG_serverName + '.' + @DG_databaseName + '.dbo.AppLog WHERE ProcessExecutionID = ' + CONVERT(VARCHAR(10),@DG_processExecutionId) + '),
												' + CONVERT(VARCHAR(10),@DG_processExecutionId) + ',
												'''',
												GETDATE(),
												''Error'',
												''Process stopped by an external app'',
												(SELECT DISTINCT Computer FROM ' + @DG_serverName + '.' + @DG_databaseName + '.dbo.AppLog WHERE ProcessExecutionID = ' + CONVERT(VARCHAR(10),@DG_processExecutionId) + '),
												(SELECT DISTINCT Operator FROM ' + @DG_serverName + '.' + @DG_databaseName + '.dbo.AppLog WHERE ProcessExecutionID = ' + CONVERT(VARCHAR(10),@DG_processExecutionId) + ')
											)';
										EXEC(@sqlScript);
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										SET @message  = 'SQL Error-08: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
										SET @SQL      = @sqlScript;
									END CATCH
								END
							
						IF(@continue = 1)
							BEGIN
								COMMIT TRANSACTION;
								SET @message = 'CANCELLED';
							END 
						ELSE
							BEGIN
								ROLLBACK TRANSACTION;
							END
					END
			END
		
	SET @status = @continue;
END
GO
