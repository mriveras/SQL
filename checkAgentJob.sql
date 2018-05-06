CREATE PROCEDURE dbo.sp_checkAgentJobs
	(
		  @sqlAgentServer      NVARCHAR(128)
		 ,@jobName             NVARCHAR(128)
		 ,@checkEveryInSeconds INT
		 ,@checkWhileStatusIs  VARCHAR(50)
	)
AS
BEGIN
/*
	CREATED BY: Mauricio Rivera
	Date: March 7 2018
	
	DESCRIPTION:
		The Following Stored Procedure checks the status of the Job specified on @jobName at the Job Agent, If the Job has the same status as
		the specified on @checkWhileStatusIs. The loop of the check will repeats until the status changes. This iterancy will be executed for the specified number on @checkWhileStatusIs which
        if used as seconds. 
        
        This Stored Procedure is been used by BI, to check in DataGovernor if a process finished.
*/
	BEGIN TRY
		DECLARE 
			 @continue         SMALLINT
			,@msg              NVARCHAR(400)
			,@jobID            NVARCHAR(128)
			,@continueChecking INT
			,@currentStatus    INT
			,@timeFrequency    VARCHAR(10)
			,@sqlScripts       NVARCHAR(MAX)
			,@int              INT;
			
		SET @continue         = 1;
		SET @msg              = '';
		SET @jobID            = '';
		
		--CHECK INPUT VARIABLES
			IF(LEN(RTRIM(LTRIM(COALESCE(@jobName,'')))) = 0)
				BEGIN
					SET @continue = 0;
					SET @msg      = N'The parameter Job Name is required';
				END
			ELSE IF(ISNULL(@checkEveryInSeconds,0) <= 0)
				BEGIN
					SET @continue = 0;
					SET @msg      = N'The parameter CheckEveryInSeconds is a required number, and must be greater than 0';
				END
	
		--TRANSFORMATIONS
			IF(@continue = 1)
				BEGIN
					IF(RTRIM(LTRIM(COALESCE(@sqlAgentServer,''))) = '')
						BEGIN
							SET @sqlAgentServer = @@SERVERNAME;
						END
					ELSE
						BEGIN
							--VALIDATE LINK SERVER CONNECTION
								BEGIN TRY
									SET @sqlScripts = N'SELECT TOP 1 @exist = 1 FROM ' + @sqlAgentServer + N'.msdb.sys.objects';
									EXEC sp_executesql @sqlScripts,N'@exist INT OUTPUT',@exist = @int OUTPUT;
									
									IF(@int <> 1)
										BEGIN
											SET @continue = 0;
											SET @msg      = N'The input Parameter @sqlAgentServer is incorrect or the Link server between the Servers ' + @@SERVERNAME + ' --> ' + @sqlAgentServer + ' does not exists';
										END
								END TRY
								BEGIN CATCH
									SET @continue = 0;
									SET @msg      = N'The input Parameter @sqlAgentServer is incorrect or the Link server between the Servers ' + @@SERVERNAME + ' --> ' + @sqlAgentServer + ' does not exists';
								END CATCH
						END
						
					IF(@checkWhileStatusIs = 'Running')
						SET @continueChecking = -1;
					ELSE IF(@checkWhileStatusIs = 'Failed')
						SET @continueChecking = 0;
					ELSE IF(@checkWhileStatusIs = 'Succeeded')
						SET @continueChecking = 1;
					ELSE IF(@checkWhileStatusIs = 'Retry')
						SET @continueChecking = 2;
					ELSE IF(@checkWhileStatusIs = 'Canceled')
						SET @continueChecking = 3;
					ELSE IF(@checkWhileStatusIs = 'Unknown')
						SET @continueChecking = 5;
					
					SET @currentStatus    = @continueChecking;
					SELECT @timeFrequency = RIGHT('0' + CAST(@checkEveryInSeconds / 3600 AS VARCHAR),2) + ':' + RIGHT('0' + CAST((@checkEveryInSeconds / 60) % 60 AS VARCHAR),2) + ':' + RIGHT('0' + CAST(@checkEveryInSeconds % 60 AS VARCHAR),2);
				END
				
		--CHECKING THE EXISTENCE OF THE JOB
			IF(@continue = 1)
				BEGIN
					--GETTING THE JOBID FROM THE NAME OF THE INPUT PARAMETER jobName
						SET @sqlScripts = N'SELECT @internalJobID = a.job_id FROM ' + @sqlAgentServer + '.msdb.dbo.sysjobs a WHERE a.name = ''' + @jobName + '''';
						EXEC sp_executesql @sqlScripts,N'@internalJobID NVARCHAR(128) OUTPUT',@internalJobID = @jobID OUTPUT;

					--IF JOB_ID IS BLANK THE JOB DOES NOT EXISTS
						IF(RTRIM(LTRIM(ISNULL(@jobID,N''))) = N'')
							BEGIN
								SET @continue = 0;
								SET @msg      = N'The Job does not exists';
							END 
				END
	
			IF(@continue = 1)
				BEGIN
				--CHECKING STATUS
					WHILE (@currentStatus = @continueChecking)
						BEGIN
							SET @sqlScripts = N'SELECT
								@internalCurrentStatus = CASE
									WHEN (a.run_Requested_date IS NOT NULL AND a.stop_execution_date IS NULL) THEN -1
									ELSE b.last_run_outcome	
								END
							FROM 
								' + @sqlAgentServer + '.msdb.dbo.sysjobactivity a INNER JOIN ' + @sqlAgentServer + '.msdb.dbo.sysjobsteps b ON
									b.job_id = a.job_id
							WHERE 
								a.job_id = ''' + @jobID + '''
								AND a.run_Requested_date = (
									SELECT MAX(run_Requested_date) 
									FROM ' + @sqlAgentServer + '.msdb.dbo.sysjobactivity 
									WHERE job_id = ''' + @jobID + '''
								)';
								
							EXEC sp_executesql @sqlScripts,N'@internalCurrentStatus INT OUTPUT',@internalCurrentStatus = @currentStatus OUTPUT;
								
							IF(@currentStatus = @continueChecking)
								WAITFOR DELAY @timeFrequency;
						END
	
					IF(@currentStatus = -1)
						BEGIN
							SET @msg = 'The Agent Job ' + @jobName + ' is running'
							RAISERROR(@msg,10,1)
						END
					ELSE if(@currentStatus = 0)
						BEGIN
							SET @msg = 'The Agent Job ' + @jobName + ' has failed'
							RAISERROR(@msg,11,1)
						END
					ELSE if(@currentStatus = 1)
						BEGIN
							SET @msg = 'The Agent Job ' + @jobName + ' succeeded'
							RAISERROR(@msg,10,1)
						END
					ELSE if(@currentStatus = 2)
						BEGIN
							SET @msg = 'The Agent Job ' + @jobName + ' has a retry status'
							RAISERROR(@msg,11,1)
						END
					ELSE if(@currentStatus = 3)
						BEGIN
							SET @msg = 'The Agent Job ' + @jobName + ' has been canceled'
							RAISERROR(@msg,10,1)
						END
					ELSE if(@currentStatus = 5)
						BEGIN
							SET @msg = 'The Agent Job ' + @jobName + ' has an Unknown status'
							RAISERROR(@msg,10,1)
						END
				END
				
		--RAISE AN ERROR IN CASE OF @continue is 0
			IF(@continue = 0)
				BEGIN
					RAISERROR(@msg,11,1)
				END
	END TRY
	BEGIN CATCH
		SET @msg = ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),N'') + N' - '+ ISNULL(ERROR_MESSAGE(),N'');
		RAISERROR(@msg,11,1);
	END CATCH
END
GO
