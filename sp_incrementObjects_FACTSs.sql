CREATE PROCEDURE [dbo].[sp_incrementObjects_FACTSs] 
	(
		 @fact_schema                NVARCHAR(128)
		,@fact_name                  NVARCHAR(128)
		,@factHash_schema            NVARCHAR(128)
		,@factHash_name              NVARCHAR(128)
		,@dimHashIndex_schema        NVARCHAR(128)
		,@dimHashIndex_name          NVARCHAR(128)
		,@dateColumn                 NVARCHAR(128) = ''
		,@monthsBack                 NVARCHAR(2)   = 0
		,@validateDimHasAssignations BIT           = 0
		,@loggingType                SMALLINT      = 3 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
		,@debug                      SMALLINT      = 0
	)
AS
/*
	Developed by: Mauricio Rivera
	Date: 17 Apr 2018
	
	MODIFICATIONS
		
		
	LAST USED LOGGING IDS:
		- ERRORS      (COD-4700E)
		- INFORMATION (COD-2800I)
*/
BEGIN
	--Transforming input parameter from NULL to default value
		IF(@dateColumn IS NULL)
			SET @dateColumn = '';
		
		IF(@monthsBack IS NULL)
			SET @monthsBack = '';
		
		IF(@loggingType IS NULL)
			SET @loggingType = 3;
			
		IF(@debug IS NULL)
			SET @debug = 0;
	
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
				SQL         VARCHAR (4000) NOT NULL,
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
			,SQL         VARCHAR (4000)
			,variables   VARCHAR (2500)
		);
	
	DECLARE
	--PROCESS FLOW VARIABLES
		 @continue            TINYINT        = 1
		,@sqlScripts          NVARCHAR(MAX)  = N''
		,@INT                 INT            = 0
		,@NVARCHAR            NVARCHAR(1000) = N''
	--LOGGING VARIABLES
		,@executionID         BIGINT         = NEXT VALUE FOR dbo.sq_BI_log_executionID
		,@execObjectName      VARCHAR(256)   = 'dbo.sp_incrementObjects_FACTSs'
		,@scriptCode          VARCHAR(25)    = ''
		,@status              VARCHAR(50)    = ''
		,@logTreeLevel        TINYINT        = 0
		,@logSpaceTree        NVARCHAR(5)    = '    '
		,@message             VARCHAR(500)   = ''
		,@SQL                 VARCHAR(4000)  = ''
	--FLAGS VARIABLES
		,@FactHash            TINYINT        = 0
		,@changesFound        TINYINT        = 0
		,@dateColumnSpecified TINYINT        = 0
		,@dateColumnIsNumeric TINYINT        = 0
		,@factHashIsNew       TINYINT        = 0
		,@variables           VARCHAR(2500)  = ''
	--GENERAL VARIABLES
		,@dimHashIndexFull    NVARCHAR(256)  = N''
		,@fromObjectFull      NVARCHAR(256)  = N''
		,@fromTempObject      NVARCHAR(128)  = N''
		,@fromTempObjectFull  NVARCHAR(256)  = N''
		,@toObjectFull        NVARCHAR(256)  = N''
		,@excludedColumns     NVARCHAR(256)  = N'ProcessExecutionID,LoadDateTime,BookCalendarSKey,DepartureSKey'
		,@HIDateColumn        NVARCHAR(128)  = N''
		,@HIHashColumn        NVARCHAR(128)  = N''
		,@HITimeType          NVARCHAR(15)   = N''
		,@HITimeUnits         NVARCHAR(10)   = N'';
	
	--INITIALIZING VARIABLES
		SET @dimHashIndexFull    = @dimHashIndex_schema + N'.' + @dimHashIndex_name;
		SET @fromObjectFull      = @fact_schema         + N'.' + @fact_name;
		SET @fromTempObject      = @fact_name           + N'_TMP';
		SET @fromTempObjectFull  = @fact_schema         + N'.' + @fact_name + N'_TMP';
		SET @toObjectFull        = @factHash_schema     + N'.' + @factHash_name;
	
	--VARIABLES FOR LOGGING
		SET @variables = ' | @fact_schema = '         + ISNULL(CAST(@fact_schema         AS VARCHAR(128)),'') + 
						 ' | @fact_name = '           + ISNULL(CAST(@fact_name           AS VARCHAR(128)),'') + 
						 ' | @factHash_schema = '     + ISNULL(CAST(@factHash_schema     AS VARCHAR(128)),'') + 
						 ' | @factHash_name = '       + ISNULL(CAST(@factHash_name       AS VARCHAR(128)),'') + 
						 ' | @dimHashIndex_schema = ' + ISNULL(CAST(@dimHashIndex_schema AS VARCHAR(128)),'') +  
						 ' | @dimHashIndex_name = '   + ISNULL(CAST(@dimHashIndex_name   AS VARCHAR(128)),'') +
						 ' | @dateColumn = '          + ISNULL(CAST(@dateColumn          AS VARCHAR(128)),'') + 
						 ' | @monthsBack = '          + ISNULL(CAST(@monthsBack          AS VARCHAR(2))  ,'') + 
						 ' | @loggingType = '         + ISNULL(CAST(@loggingType         AS VARCHAR(1))  ,'') + 
						 ' | @debug = '               + ISNULL(CAST(@debug               AS VARCHAR(1))  ,'') + 
						 ' | @changesFound = '        + ISNULL(CAST(@changesFound        AS VARCHAR(1))  ,'') + 
						 ' | @dateColumnSpecified = ' + ISNULL(CAST(@dateColumnSpecified AS VARCHAR(1))  ,'') + 
						 ' | @dateColumnIsNumeric = ' + ISNULL(CAST(@dateColumnIsNumeric AS VARCHAR(1))  ,'') + 
						 ' | @factHashIsNew = '       + ISNULL(CAST(@factHashIsNew       AS VARCHAR(1))  ,'') + 
						 ' | @dimHashIndexFull = '    + ISNULL(CAST(@dimHashIndexFull    AS VARCHAR(256)),'') + 
						 ' | @fromObjectFull = '      + ISNULL(CAST(@fromObjectFull      AS VARCHAR(256)),'') + 
						 ' | @fromTempObject = '      + ISNULL(CAST(@fromTempObject      AS VARCHAR(128)),'') + 
						 ' | @fromTempObjectFull = '  + ISNULL(CAST(@fromTempObjectFull  AS VARCHAR(256)),'') + 
						 ' | @toObjectFull = '        + ISNULL(CAST(@toObjectFull        AS VARCHAR(256)),'') + 
						 ' | @excludedColumns = '     + ISNULL(CAST(@excludedColumns     AS VARCHAR(256)),''); 
	
	--DECLARING CURSOR USED BY THE AS AT DATE HASH INDEX COLUMNS
		IF(CURSOR_STATUS('global','asAtDateCursor')>=-1)
			BEGIN
				DEALLOCATE asAtDateCursor;
			END
		
		DECLARE asAtDateCursor SCROLL CURSOR FOR (
			SELECT
				 a.HIDateColumnName
				,a.HIHashColumnName
				,a.HITimeType
				,CONVERT(NVARCHAR(10),a.HITimeUnits)
			FROM
				dbo.BIConfig_DimHashIndexAsAtDate a
			WHERE
				    a.disabled    = 0
				AND a.HITableName = @dimHashIndexFull
		);
		
		OPEN asAtDateCursor;

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
			
	--CREATING THE ROLLBACK FLAG
		BEGIN TRANSACTION;
			
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 2;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Input Parameter Validation';
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
	
	--VALIDATING INPUT PARAMETERS
		IF(SCHEMA_ID(@fact_schema) IS NULL)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-100E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The input parameter @fact_schema is not valid';
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
		ELSE IF(OBJECT_ID(@fact_schema + N'.' + @fact_name) IS NULL)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-200E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The input parameter @fact_name is not valid';
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
		ELSE IF(SCHEMA_ID(@factHash_schema) IS NULL)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-300E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The input parameter @factHash_schema is not valid';
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
			EXISTS(
				SELECT 1
				FROM sys.objects a
				WHERE 
					    a.object_id = OBJECT_ID(@factHash_schema + N'.' + @factHash_name)
					AND a.type NOT IN ('U')
			)
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-400E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The input parameter @factHash_name is not valid. If the Fact Hash object exist must be a valid Table';
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
		ELSE IF(@fact_schema = @factHash_schema AND @fact_name = @factHash_name)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-500E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Fact object should not be the same as the Fact Hash table';
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
	
	--VALIDATING FACT HASH TABLE
		IF(@continue = 1 AND OBJECT_ID(@toObjectFull) IS NOT NULL)
			BEGIN
				SET @FactHash = 1;
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 3;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Fact Hash table found';
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
			
	--IF @dateColumn IS SPECIFIED, VALIDATE ITS EXISTENCE ON THE SRC TABLE
		IF(@continue = 1 AND LEN(RTRIM(LTRIM(@dateColumn))) > 0)
			BEGIN
				IF(
					NOT EXISTS(
						SELECT 1 
						FROM 
							sys.columns a INNER JOIN sys.types b ON
								    b.system_type_id = a.system_type_id
								AND b.user_type_id   = a.user_type_id
						WHERE 
						   	    a.OBJECT_ID = OBJECT_ID(@fromObjectFull)
							AND a.name      = @dateColumn
							AND b.name     IN ('date','datetime','smalldatetime','datetime2','INT','BIGINT')
					)
				)
					BEGIN
						SET @continue = 0;
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							SET @logTreeLevel = 3;
							SET @scriptCode   = 'COD-600E';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Input Parameter @dateColumn does not exist on the Fact (Source) table or has not a valid DateTime data type';
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
						SET @dateColumnSpecified = 1;
						
						IF(
							EXISTS(
								SELECT b.name 
								FROM 
									sys.columns a INNER JOIN sys.types b ON
										    b.system_type_id = a.system_type_id
										AND b.user_type_id   = a.user_type_id
								WHERE 
								   	    a.OBJECT_ID = OBJECT_ID(@fromObjectFull)
									AND a.name      = @dateColumn
									AND b.name     IN ('INT','BIGINT')
							)
						)
							BEGIN
								SET @dateColumnIsNumeric = 1;
							END
					END
			END
	
	--IF DATE COLUMN IS ESPECIFIED, MONTHS BACK PARAMETER IS REQUIRED
		IF(@continue = 1 AND @dateColumnSpecified = 1 AND (ISNUMERIC(@monthsBack) = 0 OR @monthsBack = 0))
			BEGIN
				SET @continue            = 0;
				SET @dateColumnSpecified = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-700E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Input Parameter @monthsBack is required when the parameter @dateColumn is specified';
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
	
	IF(@debug = 1 AND @continue = 1)
		BEGIN		
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				IF(@debug = 1)
					BEGIN
						SET @logTreeLevel = 3;
						SET @scriptCode   = '';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Input Parameters Validated Sucessfully';
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
				SET @logTreeLevel = 2;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Input Parameter Validation';
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
	
	--CHECK IF A PREVIOUS PROCESS WAS EXECUTED TODAY IF IS NOT THE FIRST EXECUTION
		IF(
			@continue = 1
			AND OBJECT_ID(@dimHashIndexFull) IS NOT NULL
		)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Check today previous execution';
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
					
					SET @sqlScripts = 'SELECT @INTint = COUNT(*) FROM ' + @dimHashIndexFull + ' a WHERE a.AsAtCalendarSKey = CAST(CONVERT(VARCHAR(8),GETDATE(),112) AS INT)';
		  			
		  			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-100I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		  							
					EXEC sp_executesql @sqlScripts, N'@INTint INT OUTPUT', @INTint = @INT OUTPUT;
					
					IF(@INT = 0)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'No previous execution detected on the date ' + CONVERT(VARCHAR(8),GETDATE(),112);
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
					ELSE
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Previous execution data found on today date. Proceed to delete records';
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
												SET @logTreeLevel = 3;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Delete Previous execution rows on ' + @dimHashIndexFull;
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
										SET @sqlScripts = 'DELETE FROM ' + @dimHashIndexFull + ' WHERE AsAtCalendarSKey = CAST(CONVERT(VARCHAR(8),GETDATE(),112) AS INT)';
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 4;
													SET @scriptCode   = 'COD-200I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
													SET @status       = 'Information';
													SET @SQL          = @sqlScripts;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										
										EXEC(@sqlScripts);
										SET @INT = @@ROWCOUNT;
										
										IF(@INT > 0)
											BEGIN
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 4;
															SET @scriptCode   = '';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + CAST(@INT AS VARCHAR(10)) + ' Rows Affected';
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
										ELSE
											BEGIN
												SET @continue = 0;
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													SET @logTreeLevel = 4;
													SET @scriptCode   = 'COD-800E';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error while trying to delete record on the Table ' + @dimHashIndexFull;
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
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 4;
											SET @scriptCode   = 'COD-900E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to delete record on the Table ' + @dimHashIndexFull;
											SET @status       = 'ERROR';
											SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
												SET @logTreeLevel = 3;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Delete Previous execution rows on ' + @dimHashIndexFull;
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
							
							IF(
								@continue = 1
								AND EXISTS(
									SELECT 1
									FROM sys.objects a
									WHERE a.object_id = OBJECT_ID(@toObjectFull)
								)
							)
								BEGIN
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 3;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Delete orphans rows on ' + @toObjectFull;
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
										SET @sqlScripts = 'DELETE a FROM ' + @toObjectFull + ' a LEFT JOIN ' + @dimHashIndexFull + ' b ON b.BI_HFR = a.BI_HFR WHERE b.BI_HFR IS NULL';
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 4;
													SET @scriptCode   = 'COD-300I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
													SET @status       = 'Information';
													SET @SQL          = @sqlScripts;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										
										EXEC(@sqlScripts);
										SET @INT = @@ROWCOUNT;
										
										IF(@INT > 0)
											BEGIN
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 4;
															SET @scriptCode   = '';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + CAST(@INT AS VARCHAR(10)) + ' Rows Affected';
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
										ELSE
											BEGIN
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 4;
															SET @scriptCode   = '';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'No Rows Affected';
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
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 4;
											SET @scriptCode   = 'COD-1000E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to delete record on the Table ' + @toObjectFull;
											SET @status       = 'ERROR';
											SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,11,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END CATCH
								END
								
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Delete orphans rows on ' + @toObjectFull;
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
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Check today previous execution';
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
	
	--REPLACING DIMS IDS WITH HASH DIMS
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Creating ' + @fromTempObjectFull + ' Table';
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
					IF(OBJECT_ID(@fromTempObjectFull) IS NOT NULL)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @fromTempObjectFull + ' Table found';
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
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'DROP Table ' + @fromTempObjectFull;
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
							
							SET @sqlScripts = N'DROP TABLE ' + @fromTempObjectFull;
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-400I';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
										SET @status       = 'Information';
										SET @SQL          = @sqlScripts;
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
							EXEC(@sqlScripts);

							
							IF(OBJECT_ID(@fromTempObjectFull) IS NULL)
								BEGIN
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 3;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @fromTempObjectFull + ' Table Dropped successfully';
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
						END 
					
					--THIS SET IS THERE BECAUSE THE 'WITH' REQUIRES A SEMICOLON AT THE PREVIOUS EXECUTION LINE 
						SET @continue = 1;
					
					--CREATE CTE WITH INFORMATION OF THE FACT COLUMNS AND ASSOCIATED DIMS COLUMNS 
						WITH ObjectsColumns (
							 factColumnName
							,dimColumnName
							,dimTableName
							,dimSchemaName
							,dimTableAlias
							,factColumnPosition
						) AS (
							SELECT
								 a2.factColumnName
								,COALESCE(c2.value3,b2.dimColumnName) AS dimColumnName
								,b2.dimTableName
								,b2.dimSchemaName
								,N'a' + CONVERT(NVARCHAR(3),b2.dimTableAliasNro) AS dimTableAlias
								,a2.factColumnPosition
							FROM	
								(
									SELECT
										 a1.object_id AS factoBJECTiD
										,a1.name AS factColumnName
										,a1.column_id AS factColumnPosition
									FROM
										sys.columns a1
									WHERE
										a1.object_id = OBJECT_ID(@fromObjectFull)
								
								) a2 LEFT JOIN (
									SELECT
										 a1.object_id AS dimObjectId
										,a1.dimTableName
										,a1.dimSchemaName
										,b1.name AS dimColumnName
										,a1.dimTableAliasNro + 1 AS dimTableAliasNro
									FROM
										(
											SELECT
												 a0.object_id
												,a0.name AS dimTableName
												,SCHEMA_NAME(a0.[schema_id]) AS dimSchemaName
												,ROW_NUMBER() OVER(
													ORDER BY 
														a0.name
												) AS dimTableAliasNro
											FROM
												sys.objects a0
											WHERE
												    a0.[schema_id] = SCHEMA_ID('Dim')
												AND a0.name        LIKE '%_HASH'
										) a1 INNER JOIN sys.columns b1 ON
											b1.object_id = a1.object_id
									WHERE
										b1.name NOT IN (SELECT Item FROM dbo.udf_DelimitedSplit8K(@excludedColumns,','))
								) b2 ON
									b2.dimColumnName = a2.factColumnName
								LEFT JOIN dbo.BIConfig c2 ON
									    c2.type   = 'DIM-DIMHASH-REPLACE'
									AND c2.value1 = b2.dimSchemaName
									AND c2.value2 = b2.dimTableName
									AND c2.value6 = b2.dimColumnName
								FULL OUTER JOIN dbo.BIConfig d2 ON
									    d2.type = 'DIM-DIMHASH-EXCLUDE'
									AND d2.value1 = b2.dimSchemaName
									AND d2.value2 = b2.dimTableName
									AND d2.value3 = b2.dimColumnName
							WHERE
								d2.value6 IS NULL
						)
					
					--CREATING THE SCRIPT TO SWAP THE DIMS IDS WITH DIM HASH (NOTE the following select uses the previously created CTE, and needs to be executed right after the CTE)
								SELECT
									@sqlScripts = N'SELECT ' + (
										CONVERT(NVARCHAR(max),
											STUFF(
												(
													SELECT
														CASE
															WHEN (a.dimTableAlias IS NULL) THEN N', a1.[' + a.factColumnName + N']'
															ELSE N', ' + a.dimTableAlias + N'.BI_HFR AS [' + a.dimColumnName + N'_HFR]'
														END
													FROM
														ObjectsColumns a
													ORDER BY
														a.factColumnPosition ASC 
													FOR XML PATH(''), TYPE
												).value('.', 'VARCHAR(MAX)'), 1, 2, ''
											)
										)
									)
								+ ' INTO ' + @fromTempObjectFull
								+ ' FROM ' + @fromObjectFull + N' a1 (NOLOCK) ' + (
										CONVERT(NVARCHAR(max),
											STUFF(
												(
													SELECT
														N' INNER JOIN ' + dimSchemaName + N'.' + b.dimTableName + N' ' + b.dimTableAlias + N' (NOLOCK) ON ' + b.dimTableAlias + N'.' + dimColumnName + N' = a1.' + b.factColumnName
													FROM
														ObjectsColumns b
													WHERE
														b.dimTableAlias IS NOT NULL 
													ORDER BY
														b.factColumnPosition ASC 
													FOR XML PATH(''), TYPE
												).value('.', 'VARCHAR(MAX)'), 1, 1, ''
											)
										)
									);
						IF(@dateColumnSpecified = 1)
							BEGIN
								SET @sqlScripts = @sqlScripts + N' WHERE ';
								IF(@dateColumnIsNumeric = 1)
									BEGIN
										SET @sqlScripts = @sqlScripts + N'LEN(a1.' + @dateColumn + ') = 8 ' 
										SET @sqlScripts = @sqlScripts + N'AND a1.' + @dateColumn + ' >= CONVERT(VARCHAR(8),DATEADD(MONTH,' + @monthsBack + ',GETDATE()),112) '
									END
								ELSE
									BEGIN
										SET @sqlScripts = @sqlScripts + N'a1.' + @dateColumn + ' >= DATEADD(MONTH,' + @monthsBack + ',GETDATE()) ';
									END
							END
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-500I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC(@sqlScripts)
					SET @INT = @@ROWCOUNT;
					
					--VERIFYING TMP OBJECT
						IF(OBJECT_ID(@fromTempObjectFull) IS NOT NULL)
							--CHECKING IF THE NUMBER OF ROWS BETWEEN @fromObjectFull AND @fromTempObjectFull IS THE SAME
								IF(
									@validateDimHasAssignations = 0
									OR EXISTS(
										SELECT 1
										FROM 
											sys.dm_db_partition_stats a INNER JOIN sys.dm_db_partition_stats b ON
												    a.object_id = OBJECT_ID(@fromObjectFull)
												AND b.object_id = OBJECT_ID(@fromTempObjectFull)
												AND b.row_count = a.row_count
									)
								)
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Table ' + @fromTempObjectFull + ' created successfully with ' + CAST(@INT AS VARCHAR(20)) + ' rows affected';
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
								ELSE
									BEGIN
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1100E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Table ' + @fromObjectFull + ' and the table ' + @fromTempObjectFull + ' should have the same amount of data. This error could be because one or many dimensions columns does not match';
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
								SET @continue = 0;
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-1200E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Table ' + @fromTempObjectFull + ' does not exist after their creation';
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
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1300E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to replace Dim Ids with Dim Hash Ids';
						SET @status       = 'ERROR';
						SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Creating ' + @fromTempObjectFull + ' Table';
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

	--GENERATING HASH KEY ON @fromTempObject
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Generating Hash Key in ' + @fromTempObjectFull + ' Table';
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

					SET @sqlScripts = 'EXEC dbo.sp_generateHashKey @sourceSchema = ''' + @fact_schema + ''', @sourceObjectName = ''' + @fromTempObject + ''', @destinationSchema = ''' + @fact_schema + ''', @destinationObjectName = ''' + @fromTempObject + ''', @hashKeyColumns = '''', @dateColumn = '''', @monthsBack = '''', @debug = 0, @loggingType = 3'

					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-600I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC(@sqlScripts);
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Hash key created successfully';
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
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1400E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying create the Hash Key';
						SET @status       = 'ERROR';
						SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Generating Hash Key in ' + @fromTempObjectFull + ' Table';
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
	
	--CHEKING INDEXES ON @fromTempObject
		IF(@CONTINUE = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Indexes on ' + @fact_schema + '.' + @fromTempObject;
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
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 3;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Checking Index over BI_HFR Column';
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
					SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 2, ''DL_NC_' + @fact_schema + @fromTempObject + N'_BI_HFR'',''' + @fact_schema + ''',''' + @fromTempObject + ''',''BI_HFR'','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-700I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
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
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1500E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
								SET @status       = 'ERROR';
								SET @SQL          = @SQL;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Indexes on ' + @fact_schema + '.' + @fromTempObject;
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
	
	--POPULATE FACT HAST TABLE
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Populating Fact Hash Table';
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
						FROM sys.dm_db_partition_stats a
						WHERE 
							    a.object_id = OBJECT_ID(@toObjectFull)
							AND a.row_count > 0
					)
				)
					BEGIN
						--FACT HASH TABLE DOES NOT EXIST OR NO DATA ON IT
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Fact Hash table is new or there is no data on it';
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
				
							SET @factHashIsNew = 1;
							
							IF(OBJECT_ID(@toObjectFull) IS NOT NULL)
								BEGIN
									BEGIN TRY
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Fact Hash table found, proceed to drop it';
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
										
										SET @sqlScripts = N'DROP TABLE ' + @toObjectFull;
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = 'COD-800I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
													SET @status       = 'Information';
													SET @SQL          = @sqlScripts;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										
										EXEC(@sqlScripts);
										
										IF(OBJECT_ID(@toObjectFull) IS NULL)
											BEGIN
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 3;
															SET @scriptCode   = '';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Table ' + @toObjectFull + ' dropped successfully';
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
										ELSE
											BEGIN
												SET @continue = 0;
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													SET @logTreeLevel = 3;
													SET @scriptCode   = 'COD-1600E';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to drop the table ' + @toObjectFull;
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
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1700E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to drop the Table ' + @toObjectFull;
											SET @status       = 'ERROR';
											SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,11,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END CATCH
								END
							
						--PROCEED TO CREATE FACT HAST TABLE
							IF(@continue = 1)
								BEGIN
									BEGIN TRY
										SET @sqlScripts = N'SELECT DISTINCT * INTO ' + @toObjectFull + N' FROM ' + @fromTempObjectFull;
											
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = 'COD-900I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
													SET @status       = 'Information';
													SET @SQL          = @sqlScripts;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
																	
										EXEC(@sqlScripts);
										SET @INT = @@ROWCOUNT;
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @toObjectFull + ' Table created successfully with ' + CAST(@INT AS VARCHAR(20)) + ' rows';
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
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1800E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create Fact Hash table';
											SET @status       = 'ERROR';
											SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,11,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END CATCH
								END
					END
				ELSE
					BEGIN
						--INCREMENTAL PROCESS FOR FACT HASH TABLE 
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Incremental Process for ' + @toObjectFull + ' Table';
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
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 4;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Homogenising the Table ' + @toObjectFull + ' with ' + @fromTempObjectFull;
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
										SET @sqlScripts = N'EXEC dbo.sp_homogeniseObjectStructure @objectFrom = ''' + @fromTempObjectFull + ''', @objectTo = ''' + @toObjectFull + ''', @addNewColumns = 1, @dropNonUsedColumns = 0, @alterDataType = 1, @dontLoseDataWhenDataTypeChange = 1, @status = @statusInt OUTPUT, @message = @messageInt OUTPUT, @SQL = @SQLInt OUTPUT';

										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 5;
													SET @scriptCode   = 'COD-1000I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
													SET @status       = 'Information';
													SET @SQL          = @sqlScripts;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
		  										END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										
										EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(4000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
										
										IF(@continue = 1)
											BEGIN
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 5;
															SET @scriptCode   = '';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
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
										ELSE
											BEGIN
												SET @continue = 0;
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													SET @logTreeLevel = 5;
													SET @scriptCode   = 'COD-1900E';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
													SET @status       = 'ERROR';
													SET @SQL          = @SQL;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,11,1);
												----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											END
								END TRY
								BEGIN CATCH
									SET @continue = 0;
								END CATCH
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 4;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Homogenising the Table ' + @toObjectFull + ' with ' + @fromTempObjectFull;
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
								
								--GETTING NEW RECORDS
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN to get new records for the Table ' + @toObjectFull;
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
									
										IF(OBJECT_ID('tempdb..##DHI_factNew') IS NOT NULL)
											BEGIN
												BEGIN TRY
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = '';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_factNew found. Proceed to drop it';
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
													
													SET @sqlScripts = 'DROP TABLE ##DHI_factNew';
													
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = 'COD-1100I';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
																SET @status       = 'Information';
																SET @SQL          = @sqlScripts;
																IF(@loggingType IN (1,3))
																	BEGIN
																		INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																		VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																	END
																IF(@loggingType IN (2,3))
																	RAISERROR(@message,10,1);
					  										END
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
													
													EXEC(@sqlScripts);
													
													IF(OBJECT_ID('tempdb..##DHI_factNew') IS NULL)
														BEGIN
															----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																IF(@debug = 1)
																	BEGIN
																		SET @logTreeLevel = 5;
																		SET @scriptCode   = '';
																		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_factNew dropped';
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
													ELSE
														BEGIN
															SET @continue = 0;
															----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																SET @logTreeLevel = 5;
																SET @scriptCode   = 'COD-2000E';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to drop the Temp Table ##DHI_factNew';
																SET @status       = 'ERROR';
																SET @SQL          = @SQL;
																IF(@loggingType IN (1,3))
																	BEGIN
																		INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																		VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																	END
																IF(@loggingType IN (2,3))
																	RAISERROR(@message,11,1);
															----------------------------------------------------- END INSERT LOG -----------------------------------------------------
														END
												END TRY
												BEGIN CATCH
													SET @continue = 0;
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														SET @logTreeLevel = 5;
														SET @scriptCode   = 'COD-2100E';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to drop the Temp Table ##DHI_factNew';
														SET @status       = 'ERROR';
														SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,11,1);
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
												END CATCH 
											END
										
										IF(@continue = 1)
											BEGIN	
												BEGIN TRY
													SET @sqlScripts = N'SELECT aaa.BI_HFR
																		INTO ##DHI_factNew
																		FROM
																			' + @fromTempObjectFull + ' aaa LEFT JOIN (
																				SELECT a.BI_HFR
																				FROM
																					' + @toObjectFull + ' a INNER JOIN ' + @dimHashIndexFull + ' b ON 
																						    b.AsAtCalendarSKey = ( SELECT MAX(aa.AsAtCalendarSKey) FROM ' + @dimHashIndexFull + N' aa WHERE aa.AsAtCalendarSKey <= CAST(CONVERT(VARCHAR(8),DATEADD(DAY,-1,GETDATE()),112) AS INT) )
																						AND b.BI_HFR           = a.BI_HFR
																			) bbb ON
																				bbb.BI_HFR = aaa.BI_HFR
																		WHERE
																			bbb.BI_HFR IS NULL';
			
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = 'COD-1200I';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
																SET @status       = 'Information';
																SET @SQL          = @sqlScripts;
																IF(@loggingType IN (1,3))
																	BEGIN
																		INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																		VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																	END
																IF(@loggingType IN (2,3))
																	RAISERROR(@message,10,1);
					  										END
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
													
													EXEC(@sqlScripts);

													IF(OBJECT_ID('tempdb..##DHI_factNew') IS NOT NULL)
														BEGIN
															----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																IF(@debug = 1)
																	BEGIN
																		SET @logTreeLevel = 5;
																		SET @scriptCode   = '';
																		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_factNew created successfully';
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
													ELSE
														BEGIN
															SET @continue = 0;
															----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																SET @logTreeLevel = 5;
																SET @scriptCode   = 'COD-2200E';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create the Temp Table ##DHI_factNew';
																SET @status       = 'ERROR';
																SET @SQL          = @SQL;
																IF(@loggingType IN (1,3))
																	BEGIN
																		INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																		VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																	END
																IF(@loggingType IN (2,3))
																	RAISERROR(@message,11,1);
															----------------------------------------------------- END INSERT LOG -----------------------------------------------------
														END
												END TRY
												BEGIN CATCH
													SET @continue = 0;
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														SET @logTreeLevel = 5;
														SET @scriptCode   = 'COD-4700E';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to insert rows into the Table ' + @toObjectFull;
														SET @status       = 'ERROR';
														SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,11,1);
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
												END CATCH
											END
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END to get new records for the Table ' + @toObjectFull;
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
											--TRANSFER ROWS FROM ##DHI_factNew to @toObjectFull
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 4;
															SET @scriptCode   = '';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Insert rows from ##DHI_factNew into ' + @toObjectFull + ' Table';
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
														FROM tempdb.sys.dm_db_partition_stats a
														WHERE 
															    a.object_id = OBJECT_ID('tempdb..##DHI_factNew')
															AND a.row_count > 0
													)
												)
													BEGIN
														----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
															IF(@debug = 1)
																BEGIN
																	SET @logTreeLevel = 5;
																	SET @scriptCode   = '';
																	SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'No data found in ##DHI_factNew. Nothing to insert into ' + @toObjectFull + ' Table';
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
												ELSE
													BEGIN
														BEGIN TRY
															WITH columnList(
																columns
															) AS (
																SELECT
																	STUFF(
																		(
																			SELECT
																				', a.[' + a.name + ']'
																			FROM
																				sys.columns a 
																			WHERE
																				a.object_id = OBJECT_ID(@toObjectFull)
																			ORDER BY
																				a.column_id
																			FOR XML PATH(''), TYPE
																		).value('.', 'VARCHAR(MAX)'), 1, 2, ''
																	) AS columns
															)
															SELECT
																@sqlScripts = 'INSERT INTO ' + @toObjectFull + ' (' + REPLACE(a.columns,'a.','') + ') SELECT ' + a.columns + ' FROM ' + @fromTempObjectFull + ' a INNER JOIN ##DHI_factNew b ON b.BI_HFR = a.BI_HFR'
															FROM
																columnList a;
																
															----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																IF(@debug = 1)
																	BEGIN
																		SET @logTreeLevel = 5;
																		SET @scriptCode   = 'COD-1300I';
																		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
																		SET @status       = 'Information';
																		SET @SQL          = @sqlScripts;
																		IF(@loggingType IN (1,3))
																			BEGIN
																				INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																				VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																			END
																		IF(@loggingType IN (2,3))
																			RAISERROR(@message,10,1);
							  										END
															----------------------------------------------------- END INSERT LOG -----------------------------------------------------
															
															EXEC(@sqlScripts);
															SET @INT = @@ROWCOUNT;
															
															IF(@INT > 0)
																BEGIN
																		----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																			IF(@debug = 1)
																				BEGIN
																					SET @logTreeLevel = 5;
																					SET @scriptCode   = '';
																					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The insert rows was successfully with (' + CAST(@INT AS VARCHAR(10)) + ' rows affected';
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
															ELSE
																BEGIN
																	SET @continue = 0;
																	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																		SET @logTreeLevel = 5;
																		SET @scriptCode   = 'COD-2300E';
																		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to insert rows into the table ' + @toObjectFull;
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
														END TRY
														BEGIN CATCH
															SET @continue = 0;
															----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																SET @logTreeLevel = 5;
																SET @scriptCode   = 'COD-2400E';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to insert rows into the Table ' + @toObjectFull;
																SET @status       = 'ERROR';
																SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
																IF(@loggingType IN (1,3))
																	BEGIN
																		INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																		VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																	END
																IF(@loggingType IN (2,3))
																	RAISERROR(@message,11,1);
															----------------------------------------------------- END INSERT LOG -----------------------------------------------------
														END CATCH
													END
												
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 4;
															SET @scriptCode   = '';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Insert rows from ##DHI_factNew into ' + @toObjectFull + ' Table';
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
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Incremental Process for ' + @toObjectFull + ' Table';
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
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Populating Fact Hash Table';
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
	
	--CHEKING INDEXES ON @factHash_name (Fact final)
		IF(@CONTINUE = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Indexes on ' + @toObjectFull;
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
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 3;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Checking Index over BI_HFR Column';
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
					SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 2, ''DL_NC_' + @factHash_schema + @factHash_name + N'_BI_HFR'',''' + @factHash_schema + ''',''' + @factHash_name + ''',''BI_HFR'','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1400I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
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
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-2500E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
								SET @status       = 'ERROR';
								SET @SQL          = @SQL;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Indexes on ' + @toObjectFull;
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
		
	--POPULATE DIM HASH INDEX
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Populating ' + @dimHashIndexFull + ' Table';
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
				
				--CHECKING IF THE FACT HAST TABLE IS NEW OR IF THE HASH INDEX TABLE DOES NOT EXIST OR NO DATA ON IT
					IF(
						@factHashIsNew = 1
						OR(
							NOT EXISTS(
								SELECT 1
								FROM sys.dm_db_partition_stats a
								WHERE 
									    a.object_id = OBJECT_ID(@dimHashIndexFull)
									AND a.row_count > 0
							)
						)
					)
						BEGIN
						--IF THE DIM HASH TABLE DOES NOT EXISTS, CREATE IT WITH ALL DATA FROM @fromTempObjectFull
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Table ' + @dimHashIndexFull + ' is new or there is no data on it';
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
							
							IF(OBJECT_ID(@dimHashIndexFull) IS NOT NULL)
								BEGIN
									BEGIN TRY
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @dimHashIndexFull + ' Table found, proceed to drop it';
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
										
										SET @sqlScripts = N'DROP TABLE ' + @dimHashIndexFull;
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = 'COD-1500I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
													SET @status       = 'Information';
													SET @SQL          = @sqlScripts;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										
										EXEC(@sqlScripts);
										
										IF(OBJECT_ID(@dimHashIndexFull) IS NULL)
											BEGIN
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 3;
															SET @scriptCode   = '';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @dimHashIndexFull + ' Table dropped successfully';
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
										ELSE
											BEGIN
												SET @continue = 0;
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													SET @logTreeLevel = 3;
													SET @scriptCode   = 'COD-2600E';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to drop the Table ' + @dimHashIndexFull;
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
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-2700E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to drop the Table ' + @dimHashIndexFull;
											SET @status       = 'ERROR';
											SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,11,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END CATCH
								END
							
						--PROCEED TO CREATE FACT HAST TABLE
							IF(@continue = 1)
								BEGIN
									BEGIN TRY									
										FETCH FIRST FROM asAtDateCursor INTO @HIDateColumn,@HIHashColumn,@HITimeType,@HITimeUnits;
										
										SET @sqlScripts = N'SELECT 
																DISTINCT 
																CAST(CONVERT(VARCHAR(8),GETDATE(),112) AS INT) AS [AsAtCalendarSKey]
																,BI_HFR ';
																
										WHILE (@@FETCH_STATUS = 0)
											BEGIN
												SET @sqlScripts = @sqlScripts + N',CAST(NULL AS INT) ' + @HIDateColumn + N', CAST(NULL AS VARCHAR(40))' + @HIHashColumn;
												FETCH NEXT FROM asAtDateCursor INTO @HIDateColumn,@HIHashColumn,@HITimeType,@HITimeUnits;
											END
																										
										SET @sqlScripts = @sqlScripts + N' INTO ' + @dimHashIndexFull + 
																		N' FROM ' + @fromTempObjectFull;
											
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = 'COD-1600I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
													SET @status       = 'Information';
													SET @SQL          = @sqlScripts;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
																	
										EXEC(@sqlScripts);
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @dimHashIndexFull + ' Table created successfully';
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
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-2800E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create the Table ' + @dimHashIndexFull;
											SET @status       = 'ERROR';
											SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,11,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END CATCH
								END
						END
					ELSE
						BEGIN
						--IF THE DIM HASH TABLE EXISTS, THE FOLLOWING BLOCK WILL INCREMENT IT
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Table ' + @dimHashIndexFull + ' found';
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
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN incremental process for ' + @dimHashIndexFull + ' Table';
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
							
							--GENERATE YESTERDAY UNCHANGED DATA
								IF(@continue = 1)
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN getting Yesterday Unchanged data into Temp Table ##DHI_YesterdayUnchanged';
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
											IF(OBJECT_ID('tempdb..##DHI_YesterdayUnchanged') IS NOT NULL)
												DROP TABLE ##DHI_YesterdayUnchanged;
											
											IF(@dateColumnSpecified = 0)
												BEGIN
												--Date Column not specified. ##DHI_YesterdayUnchanged Temp Table is generated empty
													SET @sqlScripts =  N'SELECT a.* INTO ##DHI_YesterdayUnchanged FROM ' + @dimHashIndexFull + N' a WHERE 1 = 0';
												END
											ELSE
												BEGIN
												--Date Column specified. Incremental process
													SET @sqlScripts =  N'SELECT a.* INTO ##DHI_YesterdayUnchanged FROM ' + @dimHashIndexFull + N' a INNER JOIN ' + @toObjectFull + N' b ON b.BI_HFR = a.BI_HFR WHERE a.AsAtCalendarSKey = ( SELECT MAX(aa.AsAtCalendarSKey) FROM ' + @dimHashIndexFull + N' aa WHERE aa.AsAtCalendarSKey <= CAST(CONVERT(VARCHAR(8),DATEADD(DAY,-1,GETDATE()),112) AS INT) ) AND b.' + @dateColumn + N' < CAST(CONVERT(VARCHAR(8),DATEADD(MONTH,' + @monthsBack + N',GETDATE()),112) AS INT)';
												END
													
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 5;
														SET @scriptCode   = 'COD-1700I';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
														SET @status       = 'Information';
														SET @SQL          = @sqlScripts;
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,10,1);
													END
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											
											EXEC(@sqlScripts);
											SET @INT = @@ROWCOUNT;
											
											IF(OBJECT_ID('tempdb..##DHI_YesterdayUnchanged') IS NOT NULL)
												BEGIN
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = '';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_YesterdayUnchanged created successfully with ' + CAST(@INT AS VARCHAR(20)) + ' rows';
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
											ELSE
												BEGIN
													SET @continue = 0;
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														SET @logTreeLevel = 5;
														SET @scriptCode   = 'COD-2900E';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_YesterdayUnchanged creation fail';
														SET @status       = 'ERROR';
														SET @SQL          = '';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,12,1);
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
												END
										END TRY 
										BEGIN CATCH
											SET @continue = 0;
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												SET @logTreeLevel = 5;
												SET @scriptCode   = 'COD-3000E';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create the Temp Table ##DHI_YesterdayUnchanged';
												SET @status       = 'ERROR';
												SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END getting Yesterday Unchanged data into Temp Table ##DHI_YesterdayUnchanged';
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
								
							--GENERATE TODAY DATA
								IF(@continue = 1)
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN getting Today data into Temp Table ##DHI_today';
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
											IF(OBJECT_ID('tempdb..##DHI_today') IS NOT NULL)
												DROP TABLE ##DHI_today;
											
											SET @sqlScripts =  N'SELECT DISTINCT CAST(CONVERT(VARCHAR(8),GETDATE(),112) AS INT) AS AsAtCalendarSKey, BI_HFR INTO ##DHI_today FROM ' + @fromTempObjectFull;
													
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 5;
														SET @scriptCode   = 'COD-1800I';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
														SET @status       = 'Information';
														SET @SQL          = @sqlScripts;
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,10,1);
													END
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											
											EXEC(@sqlScripts);
											SET @INT = @@ROWCOUNT;
											
											IF(OBJECT_ID('tempdb..##DHI_today') IS NOT NULL)
												BEGIN
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = '';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_today created successfully with ' + CAST(@INT AS VARCHAR(20)) + ' rows';
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
											ELSE
												BEGIN
													SET @continue = 0;
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														SET @logTreeLevel = 5;
														SET @scriptCode   = 'COD-3100E';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_today creation fail';
														SET @status       = 'ERROR';
														SET @SQL          = '';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,12,1);
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
												END
										END TRY 
										BEGIN CATCH
											SET @continue = 0;
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												SET @logTreeLevel = 5;
												SET @scriptCode   = 'COD-3200E';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create the Temp Table ##DHI_today';
												SET @status       = 'ERROR';
												SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END getting Today data into Temp Table ##DHI_today';
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
							
							--GENERATE COMPLETE TODAY DATA
								IF(@continue = 1)
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Complete Today data into Temp Table ##DHI_Completetoday';
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
											IF(OBJECT_ID('tempdb..##DHI_Completetoday') IS NOT NULL)
												DROP TABLE ##DHI_Completetoday;
																	
											SET @sqlScripts =  N'SELECT
																	 DISTINCT
																	 aaa.AsAtCalendarSKey
																	,aaa.BI_HFR
																INTO ##DHI_Completetoday
																FROM
																	(
																			SELECT
																				 aa.AsAtCalendarSKey
																				,aa.BI_HFR
																			FROM
																				##DHI_Today aa
																		UNION
																			SELECT
																				 a.AsAtCalendarSKey
																				,a.BI_HFR
																			FROM
																				##DHI_YesterdayUnchanged a
																	) aaa';
													
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 5;
														SET @scriptCode   = 'COD-1900I';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
														SET @status       = 'Information';
														SET @SQL          = @sqlScripts;
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,10,1);
													END
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											
											EXEC(@sqlScripts);
											SET @INT = @@ROWCOUNT;
											
											IF(OBJECT_ID('tempdb..##DHI_Completetoday') IS NOT NULL)
												BEGIN
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = '';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_Completetoday created successfully with ' + CAST(@INT AS VARCHAR(20)) + ' rows';
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
											ELSE
												BEGIN
													SET @continue = 0;
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														SET @logTreeLevel = 5;
														SET @scriptCode   = 'COD-3300E';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_Completetoday creation fail';
														SET @status       = 'ERROR';
														SET @SQL          = '';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,12,1);
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
												END
										END TRY 
										BEGIN CATCH
											SET @continue = 0;
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												SET @logTreeLevel = 5;
												SET @scriptCode   = 'COD-3400E';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create the Temp Table ##DHI_Completetoday';
												SET @status       = 'ERROR';
												SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END getting Today data into Temp Table ##DHI_Completetoday';
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
							
							--GET AS AT DATE HASH INDEX
								IF(@continue = 1)
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN getting As At Date Hash Index data';
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
										
										--HOMOGENISING OBJECT STRUCTURE BETWEEN THE TABLE @dimHashIndexFull AND THE TEMP TABLE ##DHI_Completetoday
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 5;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Homogenising data structure between ' + @dimHashIndexFull + ' Table and ##DHI_Completetoday';
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
												SET @sqlScripts = N'EXEC dbo.sp_homogeniseObjectStructure @objectFrom = ''' + @dimHashIndexFull + ''', @objectTo = ''##DHI_Completetoday'', @addNewColumns = 1, @dropNonUsedColumns = 0, @alterDataType = 1, @dontLoseDataWhenDataTypeChange = 1, @status = @statusInt OUTPUT, @message = @messageInt OUTPUT, @SQL = @SQLInt OUTPUT';
		
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 5;
															SET @scriptCode   = 'COD-2000I';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
															SET @status       = 'Information';
															SET @SQL          = @sqlScripts;
															IF(@loggingType IN (1,3))
																BEGIN
																	INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																	VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																END
															IF(@loggingType IN (2,3))
																RAISERROR(@message,10,1);
				  										END
												----------------------------------------------------- END INSERT LOG -----------------------------------------------------
												
												EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(4000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
												
												IF(@continue = 1)
													BEGIN
														----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
															IF(@debug = 1)
																BEGIN
																	SET @logTreeLevel = 5;
																	SET @scriptCode   = '';
																	SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
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
												ELSE
													BEGIN
														SET @continue = 0;
														----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
															SET @logTreeLevel = 5;
															SET @scriptCode   = 'COD-3500E';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
															SET @status       = 'ERROR';
															SET @SQL          = @SQL;
															IF(@loggingType IN (1,3))
																BEGIN
																	INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																	VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																END
															IF(@loggingType IN (2,3))
																RAISERROR(@message,11,1);
														----------------------------------------------------- END INSERT LOG -----------------------------------------------------
													END
											END TRY
											BEGIN CATCH
												SET @continue = 0;
											END CATCH
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 5;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Homogenising data structure between ' + @dimHashIndexFull + ' Table and ##DHI_Completetoday';
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
										
										--GETTING AS AT DATE HASH INDEX DATA
											IF(@continue = 1)
												BEGIN
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = '';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Getting As At Date Hash Index data';
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
													
													IF(OBJECT_ID('tempdb..##DHI_CompletetodayFinal') IS NOT NULL)
														BEGIN
															BEGIN TRY
																----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																	IF(@debug = 1)
																		BEGIN
																			SET @logTreeLevel = 6;
																			SET @scriptCode   = '';
																			SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_CompletetodayFinal found. Proceed to drop it';
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
																
																SET @sqlScripts = 'DROP TABLE ##DHI_CompletetodayFinal';
																
																----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																	IF(@debug = 1)
																		BEGIN
																			SET @logTreeLevel = 6;
																			SET @scriptCode   = 'COD-2100I';
																			SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
																			SET @status       = 'Information';
																			SET @SQL          = @sqlScripts;
																			IF(@loggingType IN (1,3))
																				BEGIN
																					INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																					VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																				END
																			IF(@loggingType IN (2,3))
																				RAISERROR(@message,10,1);
								  										END
																----------------------------------------------------- END INSERT LOG -----------------------------------------------------
																
																EXEC(@sqlScripts);
																
																IF(OBJECT_ID('tempdb..##DHI_CompletetodayFinal') IS NULL)
																	BEGIN
																		----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																			IF(@debug = 1)
																				BEGIN
																					SET @logTreeLevel = 6;
																					SET @scriptCode   = '';
																					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_CompletetodayFinal dropped';
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
																ELSE
																	BEGIN
																		SET @continue = 0;
																		----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																			SET @logTreeLevel = 6;
																			SET @scriptCode   = 'COD-3600E';
																			SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to drop the Temp Table ##DHI_CompletetodayFinal';
																			SET @status       = 'ERROR';
																			SET @SQL          = @SQL;
																			IF(@loggingType IN (1,3))
																				BEGIN
																					INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																					VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																				END
																			IF(@loggingType IN (2,3))
																				RAISERROR(@message,11,1);
																		----------------------------------------------------- END INSERT LOG -----------------------------------------------------
																	END
															END TRY
															BEGIN CATCH
																SET @continue = 0;
																----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																	SET @logTreeLevel = 6;
																	SET @scriptCode   = 'COD-3700E';
																	SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to drop the Temp Table ##DHI_factNew';
																	SET @status       = 'ERROR';
																	SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
																	IF(@loggingType IN (1,3))
																		BEGIN
																			INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																			VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																		END
																	IF(@loggingType IN (2,3))
																		RAISERROR(@message,11,1);
																----------------------------------------------------- END INSERT LOG -----------------------------------------------------
															END CATCH 
														END
													
													IF(@continue = 1)
														BEGIN
															BEGIN TRY																	
																FETCH FIRST FROM asAtDateCursor INTO @HIDateColumn,@HIHashColumn,@HITimeType,@HITimeUnits;
																
																SET @sqlScripts = N'SELECT CAST(CONVERT(VARCHAR(8),GETDATE(),112) AS INT) AS AsAtCalendarSKey,a.BI_HFR';
																
																SET @INT = 0;
																WHILE (@@FETCH_STATUS = 0)
																	BEGIN
																		SET @INT = @INT + 1;
																		SET @NVARCHAR = CONVERT(NVARCHAR(3),@INT);
																		SET @sqlScripts = @sqlScripts + N',a' + @NVARCHAR + N'.' + @HIDateColumn + N',a' + @NVARCHAR + N'.' + @HIHashColumn;
																		FETCH NEXT FROM asAtDateCursor INTO @HIDateColumn,@HIHashColumn,@HITimeType,@HITimeUnits;
																	END
																	
																FETCH FIRST FROM asAtDateCursor INTO @HIDateColumn,@HIHashColumn,@HITimeType,@HITimeUnits;
																
																SET @sqlScripts = @sqlScripts + N' INTO ##DHI_CompletetodayFinal';
																SET @sqlScripts = @sqlScripts + N' FROM ##DHI_Completetoday a';
																
																SET @INT = 0;
																WHILE (@@FETCH_STATUS = 0)
																	BEGIN
																		SET @INT = @INT + 1;
																		SET @NVARCHAR = CONVERT(NVARCHAR(3),@INT);
																		SET @sqlScripts = @sqlScripts + N' FULL OUTER JOIN (SELECT d' + @NVARCHAR + N'.AsAtCalendarSKey AS ' + @HIDateColumn + N',d' + @NVARCHAR + N'.BI_HFR AS ' + @HIHashColumn + N' FROM ' + @dimHashIndexFull + N' d' + @NVARCHAR + N' WHERE d' + @NVARCHAR + N'.AsAtCalendarSKey = CONVERT(VARCHAR(8),DATEADD(' + @HITimeType + N',' + @HITimeUnits + N',GETDATE()),112)) a' + @NVARCHAR + N' ON a' + @NVARCHAR + N'.' + @HIHashColumn + ' = a.BI_HFR';
																		FETCH NEXT FROM asAtDateCursor INTO @HIDateColumn,@HIHashColumn,@HITimeType,@HITimeUnits;
																	END
																
																CLOSE asAtDateCursor;
																
																IF (SELECT CURSOR_STATUS('global','asAtDateCursor')) >= -1
																	DEALLOCATE asAtDateCursor;
																
																----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																	IF(@debug = 1)
																		BEGIN
																			SET @logTreeLevel = 6;
																			SET @scriptCode   = 'COD-2200I';
																			SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
																			SET @status       = 'Information';
																			SET @SQL          = @sqlScripts;
																			IF(@loggingType IN (1,3))
																				BEGIN
																					INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																					VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																				END
																			IF(@loggingType IN (2,3))
																				RAISERROR(@message,10,1);
								  										END
																----------------------------------------------------- END INSERT LOG -----------------------------------------------------
																
																EXEC(@sqlScripts);
																SET @INT = @@ROWCOUNT;
																
																IF(OBJECT_ID('tempdb..##DHI_CompletetodayFinal') IS NOT NULL)
																	BEGIN
																		----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																			IF(@debug = 1)
																				BEGIN
																					SET @logTreeLevel = 5;
																					SET @scriptCode   = '';
																					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Table ##DHI_CompletetodayFinal created successfully with ' + CAST(@INT AS VARCHAR(10)) + ' rows';
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
																ELSE
																	BEGIN
																		SET @continue = 0;
																		----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																			SET @logTreeLevel = 6;
																			SET @scriptCode   = 'COD-3800E';
																			SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create temp table ##DHI_CompletetodayFinal';
																			SET @status       = 'ERROR';
																			SET @sql          = '';
																			IF(@loggingType IN (1,3))
																				BEGIN
																					INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																					VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																				END
																			IF(@loggingType IN (2,3))
																				RAISERROR(@message,11,1);
																		----------------------------------------------------- END INSERT LOG -----------------------------------------------------
																	END
															END TRY
															BEGIN CATCH
																SET @continue = 0;
																----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																	SET @logTreeLevel = 6;
																	SET @scriptCode   = 'COD-3900E';
																	SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create temp table ##DHI_CompletetodayFinal';
																	SET @status       = 'ERROR';
																	SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
																	IF(@loggingType IN (1,3))
																		BEGIN
																			INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																			VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																		END
																	IF(@loggingType IN (2,3))
																		RAISERROR(@message,11,1);
																----------------------------------------------------- END INSERT LOG -----------------------------------------------------
															END CATCH
														END
													
													--HOGOGENIZING OBJECT STRUCTURE BETWEEN ##DHI_CompletetodayFinal AND @dimHashIndexFull
														----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
															IF(@debug = 1)
																BEGIN
																	SET @logTreeLevel = 5;
																	SET @scriptCode   = '';
																	SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Homogenising data structure between ##DHI_CompletetodayFinal Table and ' + @dimHashIndexFull;
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
															SET @sqlScripts = N'EXEC dbo.sp_homogeniseObjectStructure @objectFrom = ''##DHI_CompletetodayFinal'', @objectTo = ''' + @dimHashIndexFull + ''', @addNewColumns = 1, @dropNonUsedColumns = 0, @alterDataType = 1, @dontLoseDataWhenDataTypeChange = 1, @status = @statusInt OUTPUT, @message = @messageInt OUTPUT, @SQL = @SQLInt OUTPUT';
					
															----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																IF(@debug = 1)
																	BEGIN
																		SET @logTreeLevel = 5;
																		SET @scriptCode   = 'COD-2300I';
																		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
																		SET @status       = 'Information';
																		SET @SQL          = @sqlScripts;
																		IF(@loggingType IN (1,3))
																			BEGIN
																				INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																				VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																			END
																		IF(@loggingType IN (2,3))
																			RAISERROR(@message,10,1);
							  										END
															----------------------------------------------------- END INSERT LOG -----------------------------------------------------
															
															EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(4000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
															
															IF(@continue = 1)
																BEGIN
																	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																		IF(@debug = 1)
																			BEGIN
																				SET @logTreeLevel = 5;
																				SET @scriptCode   = '';
																				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
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
															ELSE
																BEGIN
																	SET @continue = 0;
																	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																		SET @logTreeLevel = 5;
																		SET @scriptCode   = 'COD-4000E';
																		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
																		SET @status       = 'ERROR';
																		SET @SQL          = @SQL;
																		IF(@loggingType IN (1,3))
																			BEGIN
																				INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																				VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																			END
																		IF(@loggingType IN (2,3))
																			RAISERROR(@message,11,1);
																	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
																END
														END TRY
														BEGIN CATCH
															SET @continue = 0;
														END CATCH
														----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
															IF(@debug = 1)
																BEGIN
																	SET @logTreeLevel = 5;
																	SET @scriptCode   = '';
																	SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Homogenising data structure between ##DHI_CompletetodayFinal Table and ' + @dimHashIndexFull;
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
													
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = '';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Getting As At Date Hash Index data';
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
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END getting As At Date Hash Index data';
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
  
							--INSERT COMPLETE DATA INTO @dimHashIndexFull FROM ##DHI_CompletetodayFinal
								IF(@continue = 1)
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Insert Complete Data into ' + @dimHashIndexFull + ' Table';
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
										
										--CHECKING EXISTANCE OF TODAYS DATA AT @dimHashIndexFull
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 5;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Checking if the Table ' + @dimHashIndexFull + ' has todays data';
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
											
											SET @sqlScripts = N'SELECT DISTINCT @exist = COUNT(*) FROM ' + @dimHashIndexFull + N' WHERE AsAtCalendarSKey = CAST(CONVERT(VARCHAR(8),GETDATE(),112) AS INT)';
											
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 5;
														SET @scriptCode   = 'COD-2400I';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
														SET @status       = 'Information';
														SET @SQL          = @sqlScripts;
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,10,1);
													END
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											
											EXEC sp_executesql @sqlScripts, N'@exist INT OUTPUT', @exist = @INT OUTPUT;
											
											IF(@INT > 0)
												BEGIN
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = '';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Today data Found at ' + @dimHashIndexFull + ' Table with ' + CAST(@INT AS VARCHAR(20)) + N' rows. Proceed to delete todays data';
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
													
													SET @sqlScripts = 'DELETE FROM ' + @dimHashIndexFull + ' WHERE AsAtCalendarSKey = CAST(CONVERT(VARCHAR(8),GETDATE(),112) AS INT)';
													
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
															BEGIN
																SET @logTreeLevel = 5;
																SET @scriptCode   = 'COD-2500I';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
																SET @status       = 'Information';
																SET @SQL          = @sqlScripts;
																IF(@loggingType IN (1,3))
																	BEGIN
																		INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																		VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																	END
																IF(@loggingType IN (2,3))
																	RAISERROR(@message,10,1);
															END
													----------------------------------------------------- END INSERT LOG -----------------------------------------------------
													
													EXEC(@sqlScripts);
													SET @INT = @@ROWCOUNT;
													
													IF(@INT > 0)
														BEGIN
															----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																IF(@debug = 1)
																	BEGIN
																		SET @logTreeLevel = 5;
																		SET @scriptCode   = '';
																		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Today data deleted from ' + @dimHashIndexFull + ' with ' + CAST(@INT AS VARCHAR(20)) + ' rows affected';
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
													ELSE
														BEGIN
															SET @continue = 0;
															----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																SET @logTreeLevel = 5;
																SET @scriptCode   = 'COD-4100E';
																SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'No rows affected';
																SET @status       = 'ERROR';
																SET @SQL          = '';
																IF(@loggingType IN (1,3))
																	BEGIN
																		INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																		VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																	END
																IF(@loggingType IN (2,3))
																	RAISERROR(@message,12,1);
															----------------------------------------------------- END INSERT LOG -----------------------------------------------------
														END
												END

											IF(@continue = 1)
												BEGIN
													BEGIN TRY
														SET @NVARCHAR = (
															SELECT
																STUFF(
																	(
																		SELECT
																			N',[' + aa.name + N']'
																		FROM
																			(
																				SELECT a.name, a.column_id
																				FROM sys.columns a
																				WHERE a.object_id = OBJECT_ID(@dimHashIndexFull)
																			) aa INNER JOIN (
																				SELECT b.name, b.column_id
																				FROM tempdb.sys.columns b
																				WHERE b.object_id = OBJECT_ID(N'tempdb..##DHI_CompletetodayFinal')
																			) bb ON    
																				bb.name = aa.name COLLATE DATABASE_DEFAULT
																		ORDER BY
																			aa.column_id ASC
																		FOR XML PATH(''), TYPE
																	).value('.', 'VARCHAR(MAX)'), 1, 1, ''
																)
														);
													
														SET @sqlScripts =  N'INSERT INTO ' + @dimHashIndexFull + N'(' + @NVARCHAR + N') SELECT ' + @NVARCHAR + N' FROM ##DHI_CompletetodayFinal';
														
														----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
															IF(@debug = 1)
																BEGIN
																	SET @logTreeLevel = 5;
																	SET @scriptCode   = 'COD-2600I';
																	SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute script';
																	SET @status       = 'Information';
																	SET @SQL          = @sqlScripts;
																	IF(@loggingType IN (1,3))
																		BEGIN
																			INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																			VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																		END
																	IF(@loggingType IN (2,3))
																		RAISERROR(@message,10,1);
																END
														----------------------------------------------------- END INSERT LOG -----------------------------------------------------
														
														EXEC(@sqlScripts);
														SET @INT = @@ROWCOUNT;
														
														IF(@INT > 0)
															BEGIN
																----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																	IF(@debug = 1)
																		BEGIN
																			SET @logTreeLevel = 5;
																			SET @scriptCode   = '';
																			SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + CAST(@INT AS VARCHAR(20)) + ' rows affected';
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
														ELSE
															BEGIN
																SET @continue = 0;
																----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
																	SET @logTreeLevel = 5;
																	SET @scriptCode   = 'COD-4200E';
																	SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'No rows affected';
																	SET @status       = 'ERROR';
																	SET @SQL          = '';
																	IF(@loggingType IN (1,3))
																		BEGIN
																			INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																			VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																		END
																	IF(@loggingType IN (2,3))
																		RAISERROR(@message,12,1);
																----------------------------------------------------- END INSERT LOG -----------------------------------------------------
															END
													END TRY 
													BEGIN CATCH
														SET @continue = 0;
														----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
															SET @logTreeLevel = 5;
															SET @scriptCode   = 'COD-4300E';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to insert rows into the Table ' + @dimHashIndexFull;
															SET @status       = 'ERROR';
															SET @sql          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
															IF(@loggingType IN (1,3))
																BEGIN
																	INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																	VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																END
															IF(@loggingType IN (2,3))
																RAISERROR(@message,11,1);
														----------------------------------------------------- END INSERT LOG -----------------------------------------------------
													END CATCH
												END
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Insert Complete Data into ' + @dimHashIndexFull + ' Table';
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
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END incremental process for ' + @dimHashIndexFull + ' Table';
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
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Populating ' + @dimHashIndexFull + ' Table';
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
	
	--CHEKING INDEXES ON @dimHashIndex_name (Dim Hash Index)
		IF(@CONTINUE = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Indexes on ' + @dimHashIndexFull;
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
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 3;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Checking Index over BI_HFR Column';
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
					SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 2, ''DL_NC_' + @dimHashIndex_schema + @dimHashIndex_name + N'_BI_HFR'',''' + @dimHashIndex_schema + ''',''' + @dimHashIndex_name + ''',''BI_HFR'','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-2700I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
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
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-4500E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
								SET @status       = 'ERROR';
								SET @SQL          = @SQL;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 3;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Checking Index over AsAtCalendarSKey Column';
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
					SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 1, ''DL_NC_' + @dimHashIndex_schema + @dimHashIndex_name + N'_AsAtCalendarSKey'',''' + @dimHashIndex_schema + ''',''' + @dimHashIndex_name + ''',''AsAtCalendarSKey'','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-2800I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
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
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-46000E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
								SET @status       = 'ERROR';
								SET @SQL          = @SQL;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Indexes on ' + @dimHashIndexFull;
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

	--DROP PROCESS OBJECTS (Physical & Temporal tables and Cursors)
		IF(@continue = 1)
			BEGIN
				IF(OBJECT_ID(@fromTempObjectFull) IS NOT NULL)
					BEGIN
						SET @sqlScripts = N'DROP TABLE ' + @fromTempObjectFull;
						EXEC(@sqlScripts);
					END
					
				IF(OBJECT_ID('tempdb..##DHI_YesterdayUnchanged') IS NOT NULL)
					DROP TABLE ##DHI_YesterdayUnchanged;
					
				IF(OBJECT_ID('tempdb..##DHI_today') IS NOT NULL)
					DROP TABLE ##DHI_today;
					
				IF(OBJECT_ID('tempdb..##DHI_Completetoday') IS NOT NULL)
					DROP TABLE ##DHI_Completetoday;
				
				IF(OBJECT_ID('tempdb..##DHI_CompletetodayFinal') IS NOT NULL)
					DROP TABLE ##DHI_CompletetodayFinal;
				
				IF(CURSOR_STATUS('global','asAtDateCursor')>=-1)
					BEGIN
						DEALLOCATE asAtDateCursor;
					END
			END 

	--RETURN FINAL RESULT
		IF(@continue = 1)
			BEGIN 
				COMMIT TRANSACTION
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 1;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'COMMIT Transaction';
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
		ELSE
			BEGIN
				ROLLBACK TRANSACTION
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 1;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'ROLLBACK Transaction';
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

	--Inserting Log into the physical table
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO dbo.BI_log
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
