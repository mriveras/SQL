CREATE PROCEDURE dbo.sp_RBIL_reGenerateFinalTables 
	(
		 @loggingType SMALLINT      = 3 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
		,@debug       SMALLINT      = 0
	)
AS
/*
	Developed by: Mauricio Rivera
	Date: 28 Jun 2018
	
	MODIFICATIONS
		
		
	LAST USED LOGGING IDS:
		- ERRORS      (COD-700E)
		- INFORMATION (COD-200I)
*/
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
			,SQL         VARCHAR (max)
			,SQL2        VARCHAR (max)
			,SQL3        VARCHAR (max)
			,variables   VARCHAR (2500)
		);
		  
	IF(OBJECT_ID('dbo.BI_rebuildObjectsLog') IS NULL)
		BEGIN 
			CREATE TABLE dbo.BI_rebuildObjectsLog (
				 processDateTime      DATETIME       DEFAULT(GETDATE()) NOT NULL
				,executingObject      VARCHAR  (100)                    NOT NULL
				,GG_group             VARCHAR  (250)                    NOT NULL
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
			
			CREATE INDEX NDX_dboBI_rebuildObjectsLog ON dbo.BI_rebuildObjectsLog (executingObject,GG_group,layer);			
		END
	
	--Declaring User Table Variable for logging the objects to rebuild
		DECLARE @BI_rebuildObjectsLog TABLE (
			 processDateTime      DATETIME       DEFAULT(GETDATE()) NOT NULL
			,executingObject      VARCHAR  (128)                    NOT NULL
			,GG_group             VARCHAR  (250)                    NOT NULL
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
		 @sqlScript                 NVARCHAR(MAX) = N''
		,@SQLExt                    VARCHAR(MAX)  = ''
		,@SQLExt2                   VARCHAR(MAX)  = ''
		,@SQLExt3                   VARCHAR(MAX)  = ''
		,@messageExt                VARCHAR(500)  = ''
	--LOGGING VARIABLES
		,@executionID               BIGINT        = NEXT VALUE FOR dbo.sq_BI_log_executionID
		,@execObjectName            VARCHAR(256)  = 'dbo.sp_RBIL_reGenerateFinalTables'
		,@scriptCode                VARCHAR(25)   = ''
		,@status                    VARCHAR(50)   = ''
		,@logTreeLevel              TINYINT       = 0
		,@logSpaceTree              NVARCHAR(5)   = N'    '
		,@message                   VARCHAR(500)  = ''
		,@SQL                       VARCHAR(max)  = ''
		,@variables                 VARCHAR(2500) = ''
	--FLAGS VARIABLES
		,@continue                  TINYINT       = 1
	--GENERAL VARIABLES
		,@layer                     VARCHAR(50)   = 'DataLake'
		,@asAtDateProcessed         DATETIME      = ''
		,@asAtDateProcessed_varchar NVARCHAR(50)  = N''
		,@DGGroup                   VARCHAR(100)  = ''
		,@executingObject           VARCHAR(256)  = ''
	--CURSOR VARIABLES
		,@C_finalTableObjectId      INT           = 0
		,@C_finalTableSchema        NVARCHAR(128) = N''
		,@C_finalTableName          NVARCHAR(128) = N''
		,@C_CURObjectId             INT           = 0
		,@C_CURSchema               NVARCHAR(128) = N''
		,@C_CURName                 NVARCHAR(128) = N''
		,@C_HSTObjectId             INT           = 0
		,@C_HSTSchema               NVARCHAR(128) = N''
		,@C_HSTName                 NVARCHAR(128) = N''
		,@C_asAtDate                DATETIME      = ''
		,@C_finalTableIsCUR         BIT           = 0;
	
	--INITIALIZING VARIABLES
		SET @executingObject = @execObjectName;
	--VARIABLES FOR LOGGING
		SET @variables = ' | @loggingType = ' + ISNULL(CONVERT(VARCHAR(10),@loggingType),'') + 
		                 ' | @debug = '       + ISNULL(CONVERT(VARCHAR(1) ,@debug      ),''); 
	
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
	
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 1;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Transaction';
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
	
	IF(@continue = 1)
		BEGIN
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 2;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Getting the As At Date';
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
			--CHECK CONFIG TABLE
				IF(
					NOT EXISTS(
						SELECT 1
						FROM dbo.BIconfig a
						WHERE
							a.type            = 'REBUILD-AS-AT-DATE'
							AND a.value1      = 1
							AND LEN(a.value2) > 0
							AND a.disabled = 0
					)
				)
					BEGIN
						SET @continue = 0;
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							SET @logTreeLevel = 3;
							SET @scriptCode   = 'COD-100E';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'ReBuild Process is not activated. Set (1) column Value1. SELECT value1 FROM dbo.BIconfig WHERE type = ''REBUILD-AS-AT-DATE''';
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
				ELSE IF(@continue = 1)
					BEGIN
						--GET THE AS AT DATE TO REBUILD FROM THE CONFIG TABLE
							BEGIN TRY
								SET @asAtDateProcessed = (
									SELECT CONVERT(DATETIME,CONVERT(VARCHAR(50),CONVERT(DATETIME,a.value2),106))
									FROM   dbo.BIconfig a
									WHERE
										a.type            = 'REBUILD-AS-AT-DATE'
										AND a.value1      = 1
										AND a.disabled    = 0
								)
								
								--VALIDATE @asAtDateProcessed
									IF(@asAtDateProcessed > GETDATE())
										BEGIN
											SET @continue = 0;
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												SET @logTreeLevel = 3;
												SET @scriptCode   = 'COD-200E';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The ReBuild Date must not be geather than today date. SELECT value2 FROM dbo.BIconfig WHERE type = ''REBUILD-AS-AT-DATE''';
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
									ELSE
										BEGIN
											SET @asAtDateProcessed_varchar = CONVERT(VARCHAR(50),@asAtDateProcessed,106);
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 3;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'As At Date assigned (' + CONVERT(VARCHAR(50),@asAtDateProcessed,100) + ')';
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
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-300E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
									SET @status       = 'ERROR';
									SET @SQL          = ISNULL(@sqlScript,'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
									   	RAISERROR(@message,11,1);
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							END CATCH
					END
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 2;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Getting the As At Date';
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

	IF(@continue = 1)
		BEGIN
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 2;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Getting the Data Governor Group to ReBuild';
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
			--GET DATA GOVERNOR GROUP TO REBUILD FROM THE CONFIG TABLE
					BEGIN TRY
						SET @DGGroup = dbo.udf_getBIConfigParameter('REBUILD-DG-GROUP',1);
							
						--VALIDATE @DGGroup
							IF(@DGGroup IS NULL OR LEN(@DGGroup) = 0)
								BEGIN
									SET @continue = 0;
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-400E';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Group in the Config Table is not specified. Use SELECT value1 FROM dbo.BIconfig WHERE type = ''REBUILD-DG-GROUP''';
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
							ELSE IF(
								NOT EXISTS(
									SELECT 1
									FROM   dbo.DGobjects a
									WHERE  a.DG_group = @DGGroup
								)
							)
								BEGIN
									SET @continue = 0;
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-500E';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The DataGovernor Group specified in the Config Table is not a valid registered Group. Use SELECT value1 FROM dbo.BIconfig WHERE type = ''REBUILD-DG-GROUP''';
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
							ELSE
								BEGIN
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 3;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'As At Date assigned (' + CONVERT(VARCHAR(50),@asAtDateProcessed,100) + ')';
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
					END TRY
					BEGIN CATCH
						SET @continue = 0;
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							SET @logTreeLevel = 3;
							SET @scriptCode   = 'COD-600E';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
							SET @status       = 'ERROR';
							SET @SQL          = ISNULL(@sqlScript,'');
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
							   	RAISERROR(@message,11,1);
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					END CATCH
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 2;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Getting the Data Governor Group to ReBuild';
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
	
	IF(@continue = 1)
		BEGIN
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 2;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Getting Objects to Re-Generate';
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

		--GETTING THE LIST OF OBJECTS TO REBUILD IN CASE TO BE A NEW AS AT DATE. IF IS THE SAME PROCESS, KEEP THE SAME LIST OF OBJECTS
			IF(
				NOT EXISTS(
					SELECT  1
					FROM    dbo.BI_rebuildObjectsLog a 
					WHERE 
						    a.layer           = @layer
						AND a.GG_group        = @DGGroup
						AND a.executingObject = @executingObject
						AND a.asAtDate        = @asAtDateProcessed
				)
			)
				BEGIN
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'A new AsAtDate been detected. Proceeding to generate a new list of objects to rebuild.';
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
						AND GG_group        = @DGGroup
						AND executingObject = @executingObject;

					;WITH cte_finalTables(
						 DG_group
						,layer
						,objectId
						,objectSchema
						,objectName
						,finalTableIsCUR
						,jobName
						,agentJobId
						,jobExecutionSequence
						,taskId
						,status
					) AS (
						SELECT
							 DISTINCT 					
							 a.DG_group
							,a.layer
							,a.objectId
							,a.objectSchema
							,a.objectName
							,a.finalTableIsCUR
							,a.jobName
							,a.agentJobId
							,a.jobExecutionSequence
							,a.taskId
							,'UNPROCESSED'
						FROM
							dbo.DGobjects a
						WHERE
							    a.DG_group = @DGGroup
							AND a.layer    = @layer
							AND a.objectId IS NOT NULL
							AND a.taskActive = 1
							AND a.finalTableIsCUR IN ('1','0')
						 	AND (
						 		a.TaskName LIKE '%(' + dbo.udf_getBIConfigParameter('REBUILD-SOURCE-TYPE',1) + ')%'
						 		OR (
						 			    a.TaskName NOT LIKE '%(Inc)%'
						 			AND a.TaskName NOT LIKE '%(Full)%'
						 		)
						 	)
						 	AND (
						 		    a.objectName NOT LIKE '%_SRC%'
						 		AND a.objectName NOT LIKE '%_HST%'
						 		AND a.objectName NOT LIKE '%_CUR%'
						 	)
					)
					,cte_HST(
						 DG_group
						,layer
						,objectId
						,objectSchema
						,objectName
						,finalTableIsCUR
						,jobName
						,agentJobId
						,jobExecutionSequence
						,taskId
						,status
					) AS (
						SELECT
							 DISTINCT 					
							 a.DG_group
							,a.layer
							,a.objectId
							,a.objectSchema
							,a.objectName
							,a.finalTableIsCUR
							,a.jobName
							,a.agentJobId
							,a.jobExecutionSequence
							,a.taskId
							,'UNPROCESSED' AS status
						FROM
							dbo.DGobjects a
						WHERE
							    a.DG_group = @DGGroup
							AND a.layer    = @layer
							AND a.objectId IS NOT NULL
							AND a.taskActive = 1
							AND a.finalTableIsCUR IN ('1','0')
						 	AND (
						 		a.TaskName LIKE '%(' + dbo.udf_getBIConfigParameter('REBUILD-SOURCE-TYPE',1) + ')%'
						 		OR (
						 			    a.TaskName NOT LIKE '%(Inc)%'
						 			AND a.TaskName NOT LIKE '%(Full)%'
						 		)
						 	)
						 	AND a.objectName LIKE '%_HST%'
					)
					INSERT INTO @BI_rebuildObjectsLog (executingObject, GG_group, layer, finalTableObjectId, finalTableSchema, finalTableName, CURObjectId, CURSchema, CURName, HSTObjectId, HSTSchema, HSTName, finalTableIsCUR, asAtDate, DG_jobName, DG_jobAgentId, DG_jobExecutionOrder, DG_taskId, status)
						SELECT
							 DISTINCT
							 @executingObject                                          AS executingObject
							,a.DG_group
							,a.layer
							,a.objectId                                                AS finalTableObjectId
							,a.objectSchema                                            AS finalTableSchema
							,a.objectName                                              AS finalTableName
							,OBJECT_ID(a.objectSchema + N'.' + a.objectName + N'_CUR') AS CURObjectId
							,a.objectSchema                                            AS CURSchema
							,a.objectName + N'_CUR'                                    AS CURName
							,b.objectId                                                AS HSTObjectId
							,b.objectSchema                                            AS HSTSchema
							,b.objectName                                              AS HSTName
							,a.finalTableIsCUR
							,@asAtDateProcessed                                        AS asAtDate
							,a.jobName                                                 AS DG_jobName
							,a.agentJobId                                              AS DG_agentJobId
							,a.jobExecutionSequence                                    AS DG_jobExecutionOrder
							,a.taskId                                                  AS DG_taskId
							,'UNPROCESSED'                                             AS status
						FROM
							cte_finalTables a INNER JOIN cte_HST b ON
								    b.DG_group        = a.DG_group
								AND b.finalTableIsCUR = a.finalTableIsCUR
								AND b.jobName         = a.jobName
								AND b.agentJobId      = a.agentJobId
								AND b.taskId          = a.taskId;
								
						INSERT INTO dbo.BI_rebuildObjectsLog (executingObject, GG_group, layer, finalTableObjectId, finalTableSchema, finalTableName, CURObjectId, CURSchema, CURName, HSTObjectId, HSTSchema, HSTName, finalTableIsCUR, asAtDate, DG_jobName, DG_jobAgentId, DG_jobExecutionOrder, DG_taskId, status)
							SELECT
								executingObject, GG_group, layer, finalTableObjectId, finalTableSchema, finalTableName, CURObjectId, CURSchema, CURName, HSTObjectId, HSTSchema, HSTName, finalTableIsCUR, asAtDate, DG_jobName, DG_jobAgentId, DG_jobExecutionOrder, DG_taskId, status
							FROM 
								@BI_rebuildObjectsLog;

					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
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
					INSERT INTO @BI_rebuildObjectsLog (executingObject, GG_group, layer, finalTableObjectId, finalTableSchema, finalTableName, CURObjectId, CURSchema, CURName, HSTObjectId, HSTSchema, HSTName, finalTableIsCUR, asAtDate, DG_jobName, DG_jobAgentId, DG_taskId, status)
						SELECT 
							 a.executingObject
							,a.GG_group
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
							,a.status
						FROM 
							dbo.BI_rebuildObjectsLog a
						WHERE 
							    a.layer           = @layer
							AND a.GG_group        = @DGGroup
							AND a.status          = 'UNPROCESSED'
							AND a.asAtDate        = @asAtDateProcessed
							AND a.executingObject = @executingObject
						ORDER BY
							 a.finalTableSchema ASC
							,a.finalTableName   ASC;
				END
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 2;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Getting Objects to Re-Generate';
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
	
	IF(@continue = 1)
		BEGIN
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 2;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Re-Build Final Tables';
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
		  		IF (SELECT CURSOR_STATUS('GLOBAL','RBILRGFT_CURSOR')) >= -1
					DEALLOCATE RBILRGFT_CURSOR;
				
				DECLARE RBILRGFT_CURSOR CURSOR FOR
					SELECT
						 finalTableObjectId
						,finalTableSchema
						,finalTableName
						,CURObjectId
						,CURSchema
						,CURName
						,HSTObjectId
						,HSTSchema
						,HSTName
						,asAtDate
						,a.finalTableIsCUR
					FROM 
						@BI_rebuildObjectsLog a
					WHERE 
						    a.layer           = @layer
						AND a.GG_group        = @DGGroup
						AND a.executingObject = @executingObject
						AND a.status          = 'UNPROCESSED'
					ORDER BY
						 a.finalTableSchema ASC
						,a.finalTableName   ASC;
				
				OPEN RBILRGFT_CURSOR;
				
				FETCH NEXT FROM RBILRGFT_CURSOR 
				INTO @C_finalTableObjectId,@C_finalTableSchema,@C_finalTableName,@C_CURObjectId,@C_CURSchema,@C_CURName,@C_HSTObjectId,@C_HSTSchema,@C_HSTName,@C_asAtDate,@C_finalTableIsCUR;				
				
				WHILE (@continue = 1 AND @@FETCH_STATUS = 0)
					BEGIN 
						IF(
							    @continue = 1
							AND @C_finalTableIsCUR = 1
							AND OBJECT_ID(@C_CURSchema + '.' + @C_CURName) IS NULL
						)
							BEGIN
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Renaming Final (' + @C_finalTableSchema + '.' + @C_finalTableName + ') Table to CUR (' + @C_CURSchema + '.' + @C_CURName + ')';
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
								UPDATE @BI_rebuildObjectsLog
								SET status = 'RENAMING'
								WHERE 
									    GG_group           = @DGGroup
									AND layer              = @layer
									AND executingObject    = @executingObject
									AND finalTableObjectId = @C_finalTableObjectId
									AND CURObjectId        = @C_CURObjectId
									AND HSTObjectId        = @C_HSTObjectId
									AND asAtDate           = @C_asAtDate;
									
								BEGIN TRAN
								BEGIN TRY
									SET @sqlScript = 'EXEC sp_rename ''' + @C_finalTableSchema + '.' + @C_finalTableName + ''',''' + @C_CURName + ''';'
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
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
									EXEC(@sqlScript);
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Table renamed successfully';
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
									COMMIT TRAN;
								END TRY
								BEGIN CATCH
									SET @continue = 0;
									ROLLBACK TRAN;
									UPDATE @BI_rebuildObjectsLog
									SET status = 'RENAMING ERROR'
									WHERE 
										    GG_group           = @DGGroup
										AND layer              = @layer
										AND executingObject    = @executingObject
										AND finalTableObjectId = @C_finalTableObjectId
										AND CURObjectId        = @C_CURObjectId
										AND HSTObjectId        = @C_HSTObjectId
										AND asAtDate           = @C_asAtDate;
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = 4;
										SET @scriptCode   = 'COD-700E';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
										SET @status       = 'ERROR';
										SET @SQL          = ISNULL(@sqlScript,'');
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,11,1);
									----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								END CATCH
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Renaming Final Table to CUR';
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
						
						IF(@continue = 1)
							BEGIN
								UPDATE @BI_rebuildObjectsLog
								SET status = 'PROCESSING'
								WHERE 
									    GG_group           = @DGGroup
									AND layer              = @layer
									AND executingObject    = @executingObject
									AND finalTableObjectId = @C_finalTableObjectId
									AND CURObjectId        = @C_CURObjectId
									AND HSTObjectId        = @C_HSTObjectId
									AND asAtDate           = @C_asAtDate;
								BEGIN TRANSACTION
								BEGIN TRY
									SET @sqlScript = N'EXEC dbo.sp_RBIL_processFinalTable ''' + @C_CURSchema + ''',''' + @C_CURName + ''',''' + @C_HSTSchema + ''',''' + @C_HSTName + ''',''' + @C_finalTableSchema + ''',''' + @C_finalTableName + ''',''BI_beginDate'',''BI_endDate'',''' + CONVERT(VARCHAR(50),@C_asAtDate,106) + ''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT, @SQLInt2 OUTPUT, @SQLInt3 OUTPUT';
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 3;
												SET @scriptCode   = 'COD-200I';
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
									
									EXEC sp_executesql @sqlScript, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(MAX) OUTPUT,@SQLInt2 VARCHAR(MAX) OUTPUT,@SQLInt3 VARCHAR(MAX) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @messageExt OUTPUT, @SQLInt = @SQLExt OUTPUT, @SQLInt2 = @SQLExt2 OUTPUT, @SQLInt3 = @SQLExt3 OUTPUT;
									
									IF(@continue = 1)
										BEGIN
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 3;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Final Table ' + @C_finalTableSchema + '.' + @C_finalTableName + ' Rebuilt successfully';
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
											UPDATE @BI_rebuildObjectsLog
											SET status = 'SUCCESS'
											WHERE 
												    GG_group           = @DGGroup
												AND layer              = @layer
												AND executingObject    = @executingObject
												AND finalTableObjectId = @C_finalTableObjectId
												AND CURObjectId        = @C_CURObjectId
												AND HSTObjectId        = @C_HSTObjectId
												AND asAtDate           = @C_asAtDate;
										END
									ELSE
										BEGIN
											SET @continue = 0;
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 3;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Final Table ' + @C_finalTableSchema + '.' + @C_finalTableName + ' Rebuilt Fail';
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
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 3;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @messageExt;
														SET @status       = 'Information';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,SQL2,SQL3,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@SQLExt,@SQLExt2,@SQLExt3,@variables);
															END
														IF(@loggingType IN (2,3))
														   	RAISERROR(@message,10,1);
													END 
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										END
								END TRY
								BEGIN CATCH
									SET @continue = 0;
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 3;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Final Table ' + @C_finalTableSchema + '.' + @C_finalTableName + ' Rebuilt Fail';
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
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 3;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @messageExt;
												SET @status       = 'Information';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,SQL2,SQL3,variables)
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@SQLExt,@SQLExt2,@SQLExt3,@variables);
													END
												IF(@loggingType IN (2,3))
												   	RAISERROR(@message,10,1);
											END 
									----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								END CATCH
								IF(@continue = 1)
									BEGIN
										COMMIT TRANSACTION;
									END
								ELSE 
									BEGIN
										ROLLBACK TRANSACTION;
									END
								
								IF(@continue = 0)
									BEGIN
										UPDATE @BI_rebuildObjectsLog
										SET status = 'ERROR'
										WHERE 
												GG_group           = @DGGroup
											AND layer              = @layer
											AND executingObject    = @executingObject
											AND finalTableObjectId = @C_finalTableObjectId
											AND CURObjectId        = @C_CURObjectId
											AND HSTObjectId        = @C_HSTObjectId
											AND asAtDate           = @C_asAtDate;
									END
							END
							
						FETCH NEXT FROM RBILRGFT_CURSOR 
						INTO @C_finalTableObjectId,@C_finalTableSchema,@C_finalTableName,@C_CURObjectId,@C_CURSchema,@C_CURName,@C_HSTObjectId,@C_HSTSchema,@C_HSTName,@C_asAtDate,@C_finalTableIsCUR;	
					END
					
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 2;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Re-Build Final Tables';
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
		IF (SELECT CURSOR_STATUS('GLOBAL','RBILRGFT_CURSOR')) >= -1
			DEALLOCATE RBILRGFT_CURSOR;
			
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		SET @logTreeLevel = 0;
		SET @scriptCode   = '';
		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Store Procedure';
		SET @status       = 'Information';
		SET @SQL          = '';
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
				VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
			END
		IF(@loggingType IN (2,3))
			RAISERROR(@message,10,1);
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------	
	
	--UPDATING THE dbo.BI_rebuildObjectsLog TABLE
		UPDATE a
		SET a.status = b.status
		FROM
			dbo.BI_rebuildObjectsLog a INNER JOIN @BI_rebuildObjectsLog b ON
				    b.GG_group           = a.GG_group
				AND b.layer              = a.layer
				AND b.executingObject    = a.executingObject
				AND b.finalTableObjectId = a.finalTableObjectId
				AND b.CURObjectId        = a.CURObjectId
				AND b.HSTObjectId        = a.HSTObjectId
				AND b.asAtDate           = a.asAtDate;
	
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
