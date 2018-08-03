CREATE PROCEDURE [dbo].[sp_RBIL_executeDataGovernorJobs] 
(
	 @layer       VARCHAR(50) = ''
	,@debug       SMALLINT    = 0
	,@loggingType SMALLINT    = 1 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
)
/*
	Developed by: 
	Date: 
	
	MODIFICATIONS
		
		
	LAST USED LOGGING IDS:
		- ERRORS      (COD-300E)
		- INFORMATION (COD-100I)
*/
AS
BEGIN
	--checking sequence
		IF(
			NOT EXISTS(
				SELECT 1
				FROM sys.sequences 
				WHERE name = 'sq_BI_log_executionID'
			)
		)
			BEGIN
				CREATE SEQUENCE dbo.sq_BI_log_executionID
			    	START     WITH 1  
			    	INCREMENT BY   1; 
			END
	
	IF(OBJECT_ID('dbo.BI_log') IS NULL)
		BEGIN
			CREATE TABLE dbo.BI_log
			(
				executionID BIGINT         NOT NULL,
				sequenceID  INT            NOT NULL,
				logDateTime DATETIME       NOT NULL,
				object      VARCHAR (256)  NOT NULL,
				scriptCode  VARCHAR (25)   NOT NULL,
				status      VARCHAR (50)   NOT NULL,
				message     VARCHAR (500)  NOT NULL,
				SQL         VARCHAR (max)  NOT NULL,
				SQL2        VARCHAR (max)  NOT NULL,
				SQL3        VARCHAR (max)  NOT NULL,
				variables   VARCHAR (2500) NOT NULL,
				CONSTRAINT PK_BI_log PRIMARY KEY (executionID, sequenceID)
			);
		END
	
	--Declaring User Table for Log purpose
		DECLARE @BI_log TABLE (			
			 executionID BIGINT
			,sequenceID  INT IDENTITY(1,1)
			,logDateTime DATETIME
			,object      VARCHAR (256)
			,scriptCode  VARCHAR (25)
			,status      VARCHAR (50)
			,message     VARCHAR (500)
			,SQL         VARCHAR (max) DEFAULT('') NOT NULL
			,SQL2        VARCHAR (max) DEFAULT('') NOT NULL
			,SQL3        VARCHAR (max) DEFAULT('') NOT NULL
			,variables   VARCHAR (2500)
		);
	
	IF(OBJECT_ID('dbo.BI_rebuildObjectsLog') IS NULL)
		BEGIN 
			CREATE TABLE dbo.BI_rebuildObjectsLog (
				 processDateTime      DATETIME       DEFAULT(GETDATE()) NOT NULL
				,executingObject      VARCHAR  (100)                    NOT NULL
				,DG_group             VARCHAR  (250)                    NOT NULL
				,layer                VARCHAR  (50)                     NOT NULL 
				,finalTableObjectId   INT                                   NULL
				,finalTableSchema     NVARCHAR (128)                        NULL
				,finalTableName       NVARCHAR (128)                        NULL
				,CURObjectId          INT                                   NULL 
				,CURSchema            NVARCHAR (128)                        NULL 
				,CURName              NVARCHAR (128)                        NULL
				,HSTObjectId          INT                                   NULL
				,HSTSchema            NVARCHAR (128)                        NULL 
				,HSTName              NVARCHAR (128)                        NULL
				,finalTableIsCUR      TINYINT                               NULL		
				,asAtDate             DATETIME                          NOT NULL
				,DG_jobName           VARCHAR  (100)                        NULL
				,DG_jobAgentId        UNIQUEIDENTIFIER                      NULL
				,DG_jobExecutionOrder INT                                   NULL
				,DG_taskId            INT                                   NULL
				,status               VARCHAR  (50)                     NOT NULL
			);
			
			CREATE INDEX NDX_dboBI_rebuildObjectsLog ON dbo.BI_rebuildObjectsLog (executingObject,DG_group,layer);			
		END
	
	--Declaring User Table Variable for logging the objects to rebuild
		DECLARE @BI_rebuildObjectsLog TABLE (
			 processDateTime      DATETIME       DEFAULT(GETDATE()) NOT NULL
			,executingObject      VARCHAR  (128)                    NOT NULL
			,DG_group             VARCHAR  (250)                    NOT NULL
			,layer                VARCHAR  (50)                     NOT NULL
			,finalTableObjectId   INT                                   NULL
			,finalTableSchema     NVARCHAR (128)                        NULL
			,finalTableName       NVARCHAR (128)                        NULL
			,CURObjectId          INT                                   NULL 
			,CURSchema            NVARCHAR (128)                        NULL 
			,CURName              NVARCHAR (128)                        NULL
			,HSTObjectId          INT                                   NULL
			,HSTSchema            NVARCHAR (128)                        NULL 
			,HSTName              NVARCHAR (128)                        NULL
			,finalTableIsCUR      TINYINT                               NULL		
			,asAtDate             DATETIME                          NOT NULL
			,DG_jobName           VARCHAR  (100)                        NULL
			,DG_jobAgentId        UNIQUEIDENTIFIER                      NULL
			,DG_jobExecutionOrder INT                                   NULL 
			,DG_taskId            INT                                   NULL
			,status               VARCHAR  (50)                     NOT NULL
		);
	
	DECLARE
	--PROCESS FLOW VARIABLES
		 @continue                BIT           = 1
		,@sqlScript               NVARCHAR(MAX) = N''
		,@SQLExt                  VARCHAR(MAX)  = ''
		,@SQLExt2                 VARCHAR(MAX)  = ''
		,@SQLExt3                 VARCHAR(MAX)  = ''
		,@messageExt              VARCHAR(500)  = ''
		,@jobStatus               VARCHAR(50)   = ''
		,@checkEveryInSeconds     INT           = 2
		,@timeFrequency           VARCHAR(10)   = ''
		,@datetime_begin          DATETIME      = ''
		,@datetime_end            DATETIME      = ''
		,@timeElapsed             VARCHAR(50)   = ''
	--LOGGING VARIABLES
		,@executionID             INT           = NEXT VALUE FOR dbo.sq_BI_log_executionID
		,@execObjectName          VARCHAR(256)  = 'dbo.sp_RBIL_executeDataGovernorJobs'
		,@scriptCode              VARCHAR(25)   = ''
		,@status                  VARCHAR(50)   = ''
		,@logTreeLevel            TINYINT       = 0
		,@logSpaceTree            VARCHAR(5)    = '    '
		,@message                 VARCHAR(500)  = ''
		,@SQL                     VARCHAR(4000) = ''
		,@variables               VARCHAR(2500) = ''
	--FLAGS VARIABLES
	--GENERAL VARIABLES
		,@executingObject         VARCHAR(128)  = ''
		,@rebuildProcessActivated BIT           = dbo.udf_getBIConfigParameter('REBUILD-AS-AT-DATE',1)
		,@DG_asAtDate             DATETIME      = dbo.udf_getBIConfigParameter('REBUILD-AS-AT-DATE',2)
		,@DG_group                VARCHAR(50)   = dbo.udf_getBIConfigParameter('REBUILD-DG-GROUP',1)
		,@DG_rebuildSourceType    VARCHAR(10)   = dbo.udf_getBIConfigParameter('REBUILD-SOURCE-TYPE',1)
		,@C_DG_jobAgentId         VARCHAR(100)  = ''
		,@C_DG_jobName            VARCHAR(100)  = ''
		,@C_asAtDate              DATETIME      = ''
		,@C_DG_jobExecutionOrder  INT           = 0;
	
	--INICIALIZING VARIABLES
		SET @executingObject     = @execObjectName;
		SET @timeFrequency       = RIGHT('0' + CAST(@checkEveryInSeconds / 3600 AS VARCHAR),2) + ':' + RIGHT('0' + CAST((@checkEveryInSeconds / 60) % 60 AS VARCHAR),2) + ':' + RIGHT('0' + CAST(@checkEveryInSeconds % 60 AS VARCHAR),2);
	
		SET @variables           = ' | @layer = '                + ISNULL(CONVERT(VARCHAR(50),@layer               ),'') + 
	                               ' | @DG_group = '             + ISNULL(CONVERT(VARCHAR(50),@DG_group            ),'') + 
	                               ' | @DG_rebuildSourceType = ' + ISNULL(CONVERT(VARCHAR(10),@DG_rebuildSourceType),'');
	
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		SET @logTreeLevel = 0;
		SET @scriptCode   = '';
		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Store Procedure';
		SET @status       = 'Information';
		SET @SQL          = '';
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO dbo.BI_log (executionID,sequenceID,logDateTime,object,scriptCode,status,message,SQL,variables)
				VALUES (@executionID,0,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
			END
		IF(@loggingType IN (2,3))
			RAISERROR(@message,10,1);
			
		SET @execObjectName = '';--This variable is set to BLANK because it's not necessary to set the same value in all the log records	
		SET @variables      = '';--This variable is set to BLANK because it's not necessary to set the same value in all the log records
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
	
	IF(@continue = 1)
		BEGIN
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 1;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Validate Input parameters';
						SET @status       = 'Information';
						SET @SQL          = '';
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,10,1);
					END
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			
			IF(
				NOT EXISTS(
					SELECT 1
					FROM   dbo.DGobjects a
					WHERE  
						    a.layer    = @layer
						AND a.DG_group = @DG_group
						AND (
							a.jobName LIKE '%(' + @DG_rebuildSourceType + ')'
							OR (
								    a.jobName NOT LIKE '%(Inc)'
								AND a.jobName NOT LIKE '%(Full)'
							)
						)
				)
			)
				BEGIN
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 2;
						SET @scriptCode   = 'COD-100E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Layer (' + @layer + ') does not have Jobs to execute';
						SET @status       = 'ERROR';
						SET @SQL          = '';
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END
			ELSE
				BEGIN
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Input Parameters are Successfuly validated';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 1;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Validate Input parameters';
						SET @status       = 'Information';
						SET @SQL          = '';
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,10,1);
					END
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		END
	
	IF(@continue = 1)
		BEGIN
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 1;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Getting Jobs to Execute';
						SET @status       = 'Information';
						SET @SQL          = '';
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
						   	RAISERROR(@message,10,1);
					END 
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
				IF(
					NOT EXISTS(
						SELECT  1
						FROM    dbo.BI_rebuildObjectsLog a 
						WHERE 
							    a.layer           = @layer
							AND a.DG_group        = @DG_Group
							AND a.executingObject = @executingObject
							AND a.asAtDate        = @DG_asAtDate
					)
				)
					BEGIN
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 2;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'A new AsAtDate been detected. Proceeding to generate a new list of Jobs to execute';
									SET @status       = 'Information';
									SET @SQL          = '';
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
									   	RAISERROR(@message,10,1);
								END 
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------	
						
						DELETE FROM 
							dbo.BI_rebuildObjectsLog
						WHERE
							    layer           = @layer
							AND DG_group        = @DG_Group
							AND executingObject = @executingObject;
							
						
						INSERT INTO @BI_rebuildObjectsLog (
							 executingObject
							,DG_group
							,layer
							,asAtDate
							,DG_jobName
							,DG_jobAgentId
							,DG_jobExecutionOrder
							,status
						)
							SELECT
								 DISTINCT
								 @executingObject
								,a.DG_group
								,a.layer
								,@DG_asAtDate AS asAtDate
								,a.jobName
								,a.agentJobId
								,a.jobExecutionSequence
								,'NOT EXECUTED'
							FROM 
								dbo.DGobjects a
							WHERE 
								    a.layer    = @layer
								AND a.DG_group = @DG_group
								AND (
									a.jobName LIKE '%(' + @DG_rebuildSourceType + ')'
									OR (
										    a.jobName NOT LIKE '%(Inc)'
										AND a.jobName NOT LIKE '%(Full)'
									)
								);
						
						INSERT INTO dbo.BI_rebuildObjectsLog (executingObject, DG_group, layer, finalTableObjectId, finalTableSchema, finalTableName, CURObjectId, CURSchema, CURName, HSTObjectId, HSTSchema, HSTName, finalTableIsCUR, asAtDate, DG_jobName, DG_jobAgentId, DG_jobExecutionOrder, DG_taskId, status)
							SELECT
								executingObject, DG_group, layer, finalTableObjectId, finalTableSchema, finalTableName, CURObjectId, CURSchema, CURName, HSTObjectId, HSTSchema, HSTName, finalTableIsCUR, asAtDate, DG_jobName, DG_jobAgentId, DG_jobExecutionOrder, DG_taskId, status
							FROM 
								@BI_rebuildObjectsLog;
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 2;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'New list of objects generated successfully and inserted into the variable @BI_rebuildObjectsLog';
									SET @status       = 'Information';
									SET @SQL          = '';
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
									   	RAISERROR(@message,10,1);
								END 
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					END
				ELSE
					BEGIN
						INSERT INTO @BI_rebuildObjectsLog (executingObject, DG_group, layer, finalTableObjectId, finalTableSchema, finalTableName, CURObjectId, CURSchema, CURName, HSTObjectId, HSTSchema, HSTName, finalTableIsCUR, asAtDate, DG_jobName, DG_jobAgentId, DG_taskId, DG_jobExecutionOrder, status)
							SELECT 
								 a.executingObject
								,a.DG_group
								,a.layer
								,a.finalTableObjectId
								,a.finalTableSchema
								,a.finalTableName
								,a.CURObjectId
								,a.CURSchema
								,a.CURName
								,a.HSTObjectId
								,a.HSTSchema
								,a.HSTName
								,a.finalTableIsCUR
								,a.asAtDate
								,a.DG_jobName
								,a.DG_jobAgentId
								,a.DG_taskId
								,DG_jobExecutionOrder
								,a.status
							FROM 
								dbo.BI_rebuildObjectsLog a
							WHERE 
								    a.layer           = @layer
								AND a.DG_group        = @DG_group
								AND a.status          = 'NOT EXECUTED'
								AND a.asAtDate        = @DG_asAtDate
								AND a.executingObject = @executingObject
							ORDER BY
								 a.DG_jobExecutionOrder ASC;
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 2;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Previous list of objects generated detected and inserted into the variable @BI_rebuildObjectsLog';
									SET @status       = 'Information';
									SET @SQL          = '';
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
									   	RAISERROR(@message,10,1);
								END 
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					END
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 1;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Getting Jobs to Execute';
						SET @status       = 'Information';
						SET @SQL          = '';
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,10,1);
					END
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		END
	
	IF(@continue = 1)
		BEGIN
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 1;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Executing Jobs';
						SET @status       = 'Information';
						SET @SQL          = '';
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
						   	RAISERROR(@message,10,1);
					END 
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------			
			
			--CREATE CURSOR WITH UNPROCESSED OBJECTS
		  		IF (SELECT CURSOR_STATUS('GLOBAL','RBILEDGJ_CURSOR')) >= -1
					DEALLOCATE RBILEDGJ_CURSOR;

				DECLARE RBILEDGJ_CURSOR CURSOR FOR
					SELECT
						 DISTINCT
						 a.DG_jobAgentId
						,a.DG_jobName
						,a.asAtDate
						,a.DG_jobExecutionOrder
					FROM 
						@BI_rebuildObjectsLog a
					WHERE 
						    a.layer           = @layer
						AND a.DG_group        = @DG_group
						AND a.executingObject = @executingObject
						AND a.status          IN ('NOT EXECUTED','FAIL')
					ORDER BY
						 a.DG_jobExecutionOrder ASC;
				
				OPEN RBILEDGJ_CURSOR;
				
				FETCH NEXT FROM RBILEDGJ_CURSOR INTO @C_DG_jobAgentId, @C_DG_jobName, @C_asAtDate, @C_DG_jobExecutionOrder;				
				WHILE (@continue = 1 AND @@FETCH_STATUS = 0)
					BEGIN 
						IF(@continue = 1)
							BEGIN
								UPDATE @BI_rebuildObjectsLog
								SET status = 'PROCESSING'
								WHERE 
									    DG_group        = @DG_group
									AND layer           = @layer
									AND executingObject = @executingObject
									AND DG_jobAgentId   = @C_DG_jobAgentId
									AND asAtDate        = @C_asAtDate;
									
								UPDATE a
								SET a.status = b.status
								FROM
									dbo.BI_rebuildObjectsLog a INNER JOIN @BI_rebuildObjectsLog b ON 
										    b.DG_group        = a.DG_group
										AND b.layer           = a.layer
										AND b.executingObject = a.executingObject
										AND b.DG_jobAgentId   = a.DG_jobAgentId
										AND b.asAtDate        = a.asAtDate
								WHERE
									    a.DG_group        = @DG_group
									AND a.layer           = @layer
									AND a.executingObject = @executingObject
									AND a.DG_jobAgentId   = @C_DG_jobAgentId
									AND a.asAtDate        = @C_asAtDate;
								
								BEGIN TRANSACTION
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 2;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Transaction';
											SET @status       = 'Information';
											SET @SQL          = '';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
											   	RAISERROR(@message,10,1);
										END 
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								BEGIN TRY
									SET @sqlScript = N'EXEC dbo.sp_manageDGJobs ''' + @C_DG_jobAgentId + ''', ''EXECUTE'', @statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 3;
												SET @scriptCode   = 'COD-100I';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
												SET @status       = 'Information';
												SET @SQL          = ISNULL(@sqlScript,'');
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@message,10,1);
											END
									----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									SET @datetime_begin = GETDATE();
									EXEC sp_executesql @sqlScript, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(MAX) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @messageExt OUTPUT, @SQLInt = @SQLExt OUTPUT;
									
									IF(@continue = 1)
										BEGIN
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 3;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Job (' + @C_DG_jobName + ') is executing';
														SET @status       = 'Information';
														SET @SQL          = '';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
															END
														IF(@loggingType IN (2,3))
														   	RAISERROR(@message,10,1);
													END 
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											SET @jobStatus = 'EXECUTING';
											
											UPDATE @BI_rebuildObjectsLog
											SET status = @jobStatus
											WHERE 
												    DG_group        = @DG_group
												AND layer           = @layer
												AND executingObject = @executingObject
												AND DG_jobAgentId   = @C_DG_jobAgentId
												AND asAtDate        = @C_asAtDate;
												
											UPDATE a
											SET a.status = b.status
											FROM
												dbo.BI_rebuildObjectsLog a INNER JOIN @BI_rebuildObjectsLog b ON 
													    b.DG_group        = a.DG_group
													AND b.layer           = a.layer
													AND b.executingObject = a.executingObject
													AND b.DG_jobAgentId   = a.DG_jobAgentId
													AND b.asAtDate        = a.asAtDate
											WHERE
												    a.DG_group        = @DG_group
												AND a.layer           = @layer
												AND a.executingObject = @executingObject
												AND a.DG_jobAgentId   = @C_DG_jobAgentId
												AND a.asAtDate        = @C_asAtDate;
												
											WHILE (@jobStatus = 'EXECUTING')
												BEGIN
													WAITFOR DELAY @timeFrequency;
													SET @jobStatus = (
														SELECT a.ExecutionStatus
														FROM   vw_DGJobsByStatus a
														WHERE  a.agentJobID = @C_DG_jobAgentId
													);
												END
											SET @datetime_end = GETDATE();
											SET @timeElapsed = CONVERT(VARCHAR(10),(CONVERT(INT,CONVERT(FLOAT,@datetime_end) - CONVERT(FLOAT,@datetime_begin)) * 24) + DATEPART(hh, @datetime_end - @datetime_begin)) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(mi,@datetime_end - @datetime_begin)),2) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(ss,@datetime_end - @datetime_begin)),2)
											
											IF(@jobStatus = 'SUCCEEDED')
												BEGIN
													UPDATE @BI_rebuildObjectsLog
													SET status = @jobStatus
													WHERE 
														    DG_group        = @DG_group
														AND layer           = @layer
														AND executingObject = @executingObject
														AND DG_jobAgentId   = @C_DG_jobAgentId
														AND asAtDate        = @C_asAtDate;
													
													UPDATE a
													SET a.status = b.status
													FROM
														dbo.BI_rebuildObjectsLog a INNER JOIN @BI_rebuildObjectsLog b ON 
															    b.DG_group        = a.DG_group
															AND b.layer           = a.layer
															AND b.executingObject = a.executingObject
															AND b.DG_jobAgentId   = a.DG_jobAgentId
															AND b.asAtDate        = a.asAtDate
													WHERE
														    a.DG_group        = @DG_group
														AND a.layer           = @layer
														AND a.executingObject = @executingObject
														AND a.DG_jobAgentId   = @C_DG_jobAgentId
														AND a.asAtDate        = @C_asAtDate;
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														SET @logTreeLevel = 3;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Job (' + @C_DG_jobName + ') Executed successfully. Time Elapsed (hh:mm:ss): ' + @timeElapsed;
														SET @status       = 'Information';
														SET @SQL          = '';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
															END
														IF(@loggingType IN (2,3))
														   	RAISERROR(@message,10,1);
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
												END
											ELSE
												BEGIN
													SET @continue = 0;
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														SET @logTreeLevel = 3;
														SET @scriptCode   = 'COD-200E';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Job (' + @C_DG_jobName + ') Fail. Time Elapsed (hh:mm:ss): ' + @timeElapsed;
														SET @status       = 'ERROR';
														SET @SQL          = '';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
															END
														IF(@loggingType IN (2,3))
														   	RAISERROR(@message,11,1);
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
												END
										END
									ELSE
										BEGIN
											SET @continue = 0;
											SET @datetime_end = GETDATE();
											SET @timeElapsed = CONVERT(VARCHAR(10),(CONVERT(INT,CONVERT(FLOAT,@datetime_end) - CONVERT(FLOAT,@datetime_begin)) * 24) + DATEPART(hh, @datetime_end - @datetime_begin)) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(mi,@datetime_end - @datetime_begin)),2) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(ss,@datetime_end - @datetime_begin)),2)
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												SET @logTreeLevel = 3;
												SET @scriptCode   = 'COD-300E';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Job (' + @C_DG_jobName + ') Fail Time Elapsed (hh:mm:ss): ' + @timeElapsed;
												SET @status       = 'ERROR';
												SET @SQL          = '';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
													END
												IF(@loggingType IN (2,3))
												   	RAISERROR(@message,11,1);
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												SET @logTreeLevel = 3;
												SET @scriptCode   = 'COD-400E';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @messageExt;
												SET @status       = 'ERROR';
												SET @SQL          = @SQLExt;
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@SQL,@variables);
													END
												IF(@loggingType IN (2,3))
												   	RAISERROR(@message,11,1);
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										END
								END TRY
								BEGIN CATCH
									SET @continue = 0;
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-500E';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Job (' + @C_DG_jobName + ') Fail';
										SET @status       = 'ERROR';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
										   	RAISERROR(@message,11,1);
									----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-600E';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @messageExt;
										SET @status       = 'ERROR';
										SET @SQL          = @SQLExt;
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@SQL,@variables);
											END
										IF(@loggingType IN (2,3))
										   	RAISERROR(@message,11,1);
									----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								END CATCH
								IF(@continue = 1)
									BEGIN 
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 2;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'COMMIT Transaction';
													SET @status       = 'Information';
													SET @SQL          = '';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										COMMIT TRANSACTION
									END
								ELSE
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 2;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'ROLLBACK Transaction';
													SET @status       = 'Information';
													SET @SQL          = '';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										ROLLBACK TRANSACTION
									END
								
								IF(@continue = 0)
									BEGIN
										UPDATE @BI_rebuildObjectsLog
										SET status = 'FAIL'
										WHERE 
												DG_group        = @DG_group
											AND layer           = @layer
											AND executingObject = @executingObject
											AND DG_jobAgentId   = @C_DG_jobAgentId
											AND asAtDate        = @C_asAtDate;
										
										UPDATE a
										SET a.status = b.status
										FROM
											dbo.BI_rebuildObjectsLog a INNER JOIN @BI_rebuildObjectsLog b ON 
												    b.DG_group        = a.DG_group
												AND b.layer           = a.layer
												AND b.executingObject = a.executingObject
												AND b.DG_jobAgentId   = a.DG_jobAgentId
												AND b.asAtDate        = a.asAtDate
										WHERE
											    a.DG_group        = @DG_group
											AND a.layer           = @layer
											AND a.executingObject = @executingObject
											AND a.DG_jobAgentId   = @C_DG_jobAgentId
											AND a.asAtDate        = @C_asAtDate;
									END
							END
							
						FETCH NEXT FROM RBILEDGJ_CURSOR INTO @C_DG_jobAgentId, @C_DG_jobName, @C_asAtDate, @C_DG_jobExecutionOrder;
					END
				
				--UPDATING Rebuild Object Log table
					UPDATE a
					SET a.status = b.status
					FROM
						dbo.BI_rebuildObjectsLog a INNER JOIN @BI_rebuildObjectsLog b ON 
							    b.DG_group        = a.DG_group
							AND b.layer           = a.layer
							AND b.executingObject = a.executingObject
							AND b.DG_jobAgentId   = a.DG_jobAgentId
							AND b.asAtDate        = a.asAtDate;
					
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 1;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Executing Jobs';
						SET @status       = 'Information';
						SET @SQL          = '';
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
						   	RAISERROR(@message,10,1);
					END 
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		END

	--DROP PROCESS OBJECTS (Physical & Temporal tables and Cursors)
		IF (SELECT CURSOR_STATUS('GLOBAL','RBILEDGJ_CURSOR')) >= -1
			DEALLOCATE RBILEDGJ_CURSOR;
	
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		SET @logTreeLevel = 0;
		SET @scriptCode   = '';
		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Stored Procedure';
		SET @status       = 'Information';
		SET @SQL          = '';
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
				VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
			END
		IF(@loggingType IN (2,3))
			RAISERROR(@message,10,1);
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		
	--Inserting Log into the physical table
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO dbo.BI_log (
					 executionID
					,sequenceID
					,logDateTime
					,object 
					,scriptCode 
					,status 
					,message 
					,SQL 
					,SQL2
					,SQL3
					,variables 
				)
					SELECT * FROM @BI_log;
			END
			
	--RAISE ERROR IN CASE OF
		IF(@continue = 0)
			BEGIN
				DECLARE @errorMessage NVARCHAR(300);
				
				SET @errorMessage = N'PLEASE CHECK --> SELECT * FROM dbo.BI_log WHERE executionID = ' + CONVERT(NVARCHAR(20),@executionID);
				
				RAISERROR(@errorMessage,11,1);
			END 
END
GO
