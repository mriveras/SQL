IF OBJECT_ID (N'Config.sp_executeSQLinSequence') IS NOT NULL
	DROP PROCEDURE Config.sp_executeSQLinSequence
GO

CREATE PROCEDURE [Config].[sp_executeSQLinSequence] 
(
	 @groupName         VARCHAR(50) = 'ALL'
	,@funcId            INT         = 0
	,@executionID       BIGINT      = 0
	,@startLogTreeLevel TINYINT     = 0
	,@debug             BIT         = 0
	,@loggingType       BIT         = 1 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
)
/*
	Developed by: Mauricio Rivera Senior
	Date: 19 Oct 2018
	
	MODIFICATIONS
		
		
	LAST USED LOGGING IDS:
		- ERROR       (COD-E500)
		- INFORMATION (COD-I100)
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
			);
			CREATE CLUSTERED INDEX CIX_dbo_BIlog_1 ON dbo.BI_log (executionID);
		END
	
	IF OBJECT_ID (N'Config.REF_BUSINESS_ETL_QUERY') IS NULL
		BEGIN
			CREATE TABLE Config.REF_BUSINESS_ETL_QUERY
			(
				funcId                   INT IDENTITY NOT NULL,
				groupName                VARCHAR (50) NOT NULL,
				sequenceOrder            BIGINT DEFAULT ((1)) NOT NULL,
				description              VARCHAR (300) DEFAULT ('') NOT NULL,
				status                   VARCHAR (50) DEFAULT ('NEVER RUN') NOT NULL,
				beginLastExecution       DATETIME DEFAULT ('31 Dec 9999 23:59:59') NOT NULL,
				endLastExecution         DATETIME DEFAULT ('31 Dec 9999 23:59:59') NOT NULL,
				timeElapsedLastExecution VARCHAR (20) DEFAULT ('') NOT NULL,
				rowsAffected             INT DEFAULT ((0)) NOT NULL,
				variableList             NVARCHAR (200) DEFAULT ('') NOT NULL,
				odysseyList              NVARCHAR (200) DEFAULT ('') NOT NULL,
				biList                   NVARCHAR (200) DEFAULT ('') NOT NULL,
				SQL1                     NVARCHAR (max) DEFAULT ('') NOT NULL,
				SQL2                     NVARCHAR (max) DEFAULT ('') NOT NULL,
				SQL3                     NVARCHAR (max) DEFAULT ('') NOT NULL,
				disabled                 BIT DEFAULT ((0)) NOT NULL,
				CONSTRAINT PK_REF_BUSINESS_ETL_QUERY PRIMARY KEY (funcId)
			);
			CREATE INDEX CIX_Config_REF_BUSINESS_ETL_QUERY_1 ON Config.REF_BUSINESS_ETL_QUERY (groupName);
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

	DECLARE
	--PROCESS FLOW VARIABLES
		 @continue                BIT            = 1
		,@sqlScript               NVARCHAR(MAX)  = N''
	--LOGGING VARIABLES
		,@execObjectName          VARCHAR(256)   = 'Config.sp_executeSQLinSequence'
		,@scriptCode              VARCHAR(25)    = ''
		,@status                  VARCHAR(50)    = ''
		,@logTreeLevel            TINYINT        = 0
		,@logSpaceTree            VARCHAR(5)     = '    '
		,@message                 VARCHAR(500)   = ''
		,@SQL                     VARCHAR(4000)  = ''
		,@variables               VARCHAR(2500)  = ''
	--FLAGS VARIABLES
		,@continue_C              BIT            = 1
	--GENERAL VARIABLES
		,@environment             VARCHAR(10)    = ''
		,@groupName_C             VARCHAR(50)    = ''
		,@funcId_C                INT            = 0
		,@variableList_C          NVARCHAR(200)  = N''
		,@odysseyList_C           NVARCHAR(200)  = N''
		,@biList_C                NVARCHAR(200)  = N''
		,@SQL1_C                  NVARCHAR(MAX)  = N''
		,@SQL2_C                  NVARCHAR(MAX)  = N''
		,@SQL3_C                  NVARCHAR(MAX)  = N''
		,@beginDateTime_C         DATETIME       = ''
		,@endDateTime_C           DATETIME       = ''
		,@timeElapsed_C           VARCHAR(50)    = ''
		,@rowsAffected_C          INT            = 0
		,@continue_CC             BIT            = 1
		,@variable_CC             NVARCHAR(200)  = N''
		,@replace_CC              NVARCHAR(200)  = N''
		,@numChanges_CC           INT            = 0;
	
	--VARIABLES FOR LOGGING
		SET @variables = ' | @groupName = '         + ISNULL(CONVERT(VARCHAR(50),@groupName        ),'') + 
		                 ' | @funcId = '            + ISNULL(CONVERT(VARCHAR(10),@funcId           ),'') +
		                 ' | @executionID = '       + ISNULL(CONVERT(VARCHAR(20),@executionID      ),'') + 
		                 ' | @startLogTreeLevel = ' + ISNULL(CONVERT(VARCHAR(10),@startLogTreeLevel),'') + 
		                 ' | @debug = '             + ISNULL(CONVERT(VARCHAR(1) ,@debug            ),'') + 
		                 ' | @loggingType = '       + ISNULL(CONVERT(VARCHAR(1) ,@loggingType      ),'');
	
	--CHECKING THE EXECUTION ID PROVIDED AS AN INPUT PARAMETER
		IF(@executionID IS NULL OR @executionID = 0)
			BEGIN
				SET @executionID = NEXT VALUE FOR dbo.sq_BI_log_executionID;
			END
			
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		SET @logTreeLevel = @startLogTreeLevel + 0;
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
	
	--VALIDATE INPUT PARAMETERS
		----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
			IF(@debug = 1)
				BEGIN
					SET @logTreeLevel = @startLogTreeLevel + 1;
					SET @scriptCode   = '';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Validating Input Parameters';
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
			    @groupName != 'ALL'
			AND NOT EXISTS(
				SELECT 1
				FROM   Config.REF_BUSINESS_ETL_QUERY a
				WHERE  a.groupName = @groupName
			)
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = @startLogTreeLevel + 2;
					SET @scriptCode   = 'COD-100E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The specified @GroupName (' + @groupName + ') does not exist at Config.REF_BUSINESS_ETL_QUERY';
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
		ELSE IF(
			    @funcId != 0
			AND NOT EXISTS(
				SELECT 1
				FROM   Config.REF_BUSINESS_ETL_QUERY a
				WHERE  a.funcId = @funcId
			)
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = @startLogTreeLevel + 2;
					SET @scriptCode   = 'COD-200E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The specified @funcId (' + CONVERT(VARCHAR(10),@funcId) + ') does not exist at Config.REF_BUSINESS_ETL_QUERY';
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
							SET @logTreeLevel = @startLogTreeLevel + 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Input Parameters validated successfully';
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
					SET @logTreeLevel = @startLogTreeLevel + 1;
					SET @scriptCode   = '';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Validating Input Parameters';
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
	
	--GET ENVIRONMENT DETAILS
		IF(@continue = 1)
			BEGIN 
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = @startLogTreeLevel + 1;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Environment Details';
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
				
				--ODYSSEY
					IF(@@SERVERNAME IN ('HANDB1','CHEDEVDB1') AND DB_NAME() IN ('ODYSSEY','ODYSSEY_TEST','ODYSSEY_DEV','ODYSSEY_STAGING','ODYSSEY_TRAINING'))
						BEGIN
							SET @environment = 'ODYSSEY';
						END
					ELSE IF(@@SERVERNAME IN ('CHEBIZ1','CHEVBI4','CHEVBI3') AND DB_NAME() IN ('APT_DWH'))
						BEGIN
							SET @environment = 'BI';
						END
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = @startLogTreeLevel + 2;
								SET @scriptCode   = 'COD-300E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The environment not identified. The Server ' + @@SERVERNAME + ' and Database ' + DB_NAME() + ' not matched to any configuration';
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
				
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = @startLogTreeLevel + 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @environment + ' environment detected';
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
							SET @logTreeLevel = @startLogTreeLevel + 1;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Environment Details';
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
			
	--GETTING THE SQL LIST
		IF(@continue = 1)
			BEGIN
				--CREATING CURSOR WITH INDEXES TO CREATE
					IF (SELECT CURSOR_STATUS('LOCAL','ESQLIS_sql_cursor')) >= -1
						BEGIN
							DEALLOCATE ESQLIS_sql_cursor;
						END
			   	
				DECLARE ESQLIS_sql_cursor CURSOR LOCAL FOR
					SELECT
						 a.funcId
						,a.groupName
						,COALESCE(a.variableList,'')
						,COALESCE(a.odysseyList,'')
						,COALESCE(a.biList,'')
						,COALESCE(a.SQL1,'')
						,COALESCE(a.SQL2,'')
						,COALESCE(a.SQL3,'')
					FROM 
						Config.REF_BUSINESS_ETL_QUERY a (NOLOCK)
					WHERE
						a.disabled = 0
						AND (
							   a.environmentToRun = 'ALL'
							OR a.environmentToRun = @environment
						)
						AND (
							(
								    a.groupName = @groupName
								AND a.funcId    = @funcId
							)
							OR (
								    @groupName  = 'ALL'
								AND a.funcId    = @funcId
							)
							OR (
								    a.groupName = @groupName
								AND @funcId     = 0
							)
							OR (
								    @groupName  = 'ALL'
								AND @funcId     = 0
							)
						)
					ORDER BY
						 a.sequenceOrder ASC;
					   	
					OPEN ESQLIS_sql_cursor;
					
					FETCH NEXT FROM ESQLIS_sql_cursor 
					INTO @funcId_C,@groupName_C,@variableList_C,@odysseyList_C,@biList_C,@SQL1_C,@SQL2_C,@SQL3_C;					
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = @startLogTreeLevel + 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - ITERATION PROCESS';
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
					
					UPDATE 
						Config.REF_BUSINESS_ETL_QUERY
					SET 
						 status = 'WAITING'
						,beginLastExecution       = ''
						,endLastExecution         = ''
						,timeElapsedLastExecution = ''
						,rowsAffected             = 0
					WHERE
						disabled = 0
						AND (
							   environmentToRun = 'ALL'
							OR environmentToRun = @environment
						)
						AND (
							(
								    groupName = @groupName
								AND funcId    = @funcId
							)
							OR (
								    @groupName  = 'ALL'
								AND funcId    = @funcId
							)
							OR (
								    groupName = @groupName
								AND @funcId     = 0
							)
							OR (
								    @groupName  = 'ALL'
								AND @funcId     = 0
							)
						);
					
					WHILE (@@FETCH_STATUS = 0)
						BEGIN
							SET @continue_C      = 1;
							SET @beginDateTime_C = GETDATE();
							SET @endDateTime_C   = '';
							SET @timeElapsed_C   = ''; 
							SET @rowsAffected_C  = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = @startLogTreeLevel + 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - EXECUTING THE FUNCID (' + CONVERT(VARCHAR(10),@funcId_C) + ') | GROUPNAME (' + @groupName_C + ')';
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
						
						--CHANGING SCHEMAS
							IF(@continue_C = 1)
								BEGIN
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Changing Schemas';
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
									
										IF (SELECT CURSOR_STATUS('LOCAL','ESQLIS_changeSchemas')) >= -1
											BEGIN
												DEALLOCATE ESQLIS_changeSchemas;
											END
								   	
										DECLARE ESQLIS_changeSchemas CURSOR LOCAL FOR
											SELECT
												 a.variable
												,a.replace
											FROM
												(
													SELECT
														 aa.item AS variable
														,CASE
															WHEN (@environment = 'ODYSSEY') THEN bb.item	
															WHEN (@environment = 'BI'     ) THEN cc.item
															ELSE ''
														END AS replace
													FROM
														dbo.udf_DelimitedSplit8K(@variableList_C,',') aa INNER JOIN dbo.udf_DelimitedSplit8K(@odysseyList_C,',') bb ON
															bb.itemNumber = aa.itemNumber
														INNER JOIN dbo.udf_DelimitedSplit8K(@biList_C,',') cc ON
															cc.itemNumber = aa.itemNumber
												) a
											WHERE
												a.replace != '';
										
										OPEN ESQLIS_changeSchemas;
					
										FETCH NEXT FROM ESQLIS_changeSchemas INTO @variable_CC, @replace_CC;	
										
										WHILE (@@FETCH_STATUS = 0 AND @continue_CC = 1)
											BEGIN												
												BEGIN TRY
													SET @numChanges_CC = (LEN(@SQL1_C) - LEN(REPLACE(@SQL1_C,@variable_CC,''))) / LEN(@variable_CC);
													SET @SQL1_C = REPLACE(@SQL1_C,@variable_CC,@replace_CC);
													
													SET @numChanges_CC = @numChanges_CC + ((LEN(@SQL2_C) - LEN(REPLACE(@SQL2_C,@variable_CC,''))) / LEN(@variable_CC));
													SET @SQL2_C = REPLACE(@SQL2_C,@variable_CC,@replace_CC);
													
													SET @numChanges_CC = @numChanges_CC + ((LEN(@SQL3_C) - LEN(REPLACE(@SQL3_C,@variable_CC,''))) / LEN(@variable_CC));
													SET @SQL3_C = REPLACE(@SQL3_C,@variable_CC,@replace_CC);
													
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = @startLogTreeLevel + 5;
																SET @scriptCode   = '';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Variable (' + @variable_CC + ') replaced with (' + @replace_CC + '). Total replacements (' + CONVERT(VARCHAR(10),@numChanges_CC) + ')';
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
												END TRY
												BEGIN CATCH
													SET @continue    = 0;
													SET @continue_C  = 0;
													SET @continue_CC = 0;
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														SET @logTreeLevel = @startLogTreeLevel + 5;
														SET @scriptCode   = 'COD-400E';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
												END CATCH
												FETCH NEXT FROM ESQLIS_changeSchemas INTO @variable_CC, @replace_CC;
											END
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Changing Schemas';
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
						
						--EXECUTING SQL
							IF(@continue_C = 1)
								BEGIN
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Executing the Script';
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
									
									BEGIN TRY						
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = @startLogTreeLevel + 5;
													SET @scriptCode   = 'COD-100I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
													SET @status       = 'Information';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,SQL2,SQL3,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@SQL1_C,@SQL2_C,@SQL3_C,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------											
											UPDATE 
												Config.REF_BUSINESS_ETL_QUERY
											SET 
												 status = 'PROCESSING'
												,beginLastExecution       = @beginDateTime_C
												,endLastExecution         = @endDateTime_C
												,timeElapsedLastExecution = @timeElapsed_C
												,rowsAffected             = @rowsAffected_C
											WHERE
												funcId = @funcId_C;
											
											EXEC(@SQL1_C + N' ' + @SQL2_C + N' ' + @SQL3_C);
											
											SET @rowsAffected_C = @@ROWCOUNT;
											
											SET @endDateTime_C = GETDATE();

											SET @timeElapsed_C = CONVERT(VARCHAR(10),(CONVERT(INT,CONVERT(FLOAT,@endDateTime_C) - CONVERT(FLOAT,@beginDateTime_C)) * 24) + DATEPART(hh, @endDateTime_C - @beginDateTime_C)) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(mi,@endDateTime_C - @beginDateTime_C)),2) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(ss,@endDateTime_C - @beginDateTime_C)),2);
											
											UPDATE 
												Config.REF_BUSINESS_ETL_QUERY
											SET 
												 status = 'SUCCESS'
												,beginLastExecution       = @beginDateTime_C
												,endLastExecution         = @endDateTime_C
												,timeElapsedLastExecution = @timeElapsed_C
												,rowsAffected             = @rowsAffected_C
											WHERE
												funcId = @funcId_C;
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 5;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL executed successfully. Time Elapsed: ' + @timeElapsed_C + ' | Rows affected: ' + CONVERT(VARCHAR(15),@rowsAffected_C);
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
									END TRY
									BEGIN CATCH
										SET @continue_C = 0;
										
										SET @endDateTime_C = GETDATE();
										SET @timeElapsed_C = CONVERT(VARCHAR(10),(CONVERT(INT,CONVERT(FLOAT,@endDateTime_C) - CONVERT(FLOAT,@beginDateTime_C)) * 24) + DATEPART(hh, @endDateTime_C - @beginDateTime_C)) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(mi,@endDateTime_C - @beginDateTime_C)),2) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(ss,@endDateTime_C - @beginDateTime_C)),2);
										
										UPDATE 
											Config.REF_BUSINESS_ETL_QUERY
										SET 
											 status = 'SUCCESS'
											,beginLastExecution       = @beginDateTime_C
											,endLastExecution         = @endDateTime_C
											,timeElapsedLastExecution = @timeElapsed_C
										WHERE
											funcId = @funcId_C;
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = @startLogTreeLevel + 5;
											SET @scriptCode   = 'COD-500E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											SET @status       = 'ERROR';
											SET @SQL          = ISNULL(@sqlScript,'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,11,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END CATCH
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Executing the Script';
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
										SET @logTreeLevel = @startLogTreeLevel + 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - EXECUTING THE FUNCID (' + CONVERT(VARCHAR(10),@funcId_C) + ') | GROUPNAME (' + @groupName_C + ')';
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
							FETCH NEXT FROM ESQLIS_sql_cursor 
							INTO @funcId_C,@groupName_C,@variableList_C,@odysseyList_C,@biList_C,@SQL1_C,@SQL2_C,@SQL3_C;
						END
						
						CLOSE ESQLIS_sql_cursor;
						
						IF (SELECT CURSOR_STATUS('LOCAL','ESQLIS_sql_cursor')) >= -1
							BEGIN
								DEALLOCATE ESQLIS_sql_cursor;
							END
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = @startLogTreeLevel + 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - ITERATION PROCESS';
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
		SET @logTreeLevel = @startLogTreeLevel + 0;
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
