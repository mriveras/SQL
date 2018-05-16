CREATE PROCEDURE [dbo].[sp_removeDuplications] 
	(
		 @sourceSchema          NVARCHAR(128)
		,@sourceObjectName      NVARCHAR(128)
		,@destinationSchema     NVARCHAR(128)
		,@destinationObjectName NVARCHAR(128)
		,@hashKeyColumnName     NVARCHAR(128)
		,@debug                 SMALLINT      = 0
		,@loggingType           SMALLINT      = 1 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
	)
AS
/*
	Developed by: Mauricio Rivera
	Date: 10 May 2018
	
	LAST USED LOGGING IDS:
		- ERRORS      (COD-1100E)
		- INFORMATION (COD-300I)
*/
BEGIN
	--Transforming input parameter from NULL to ''
		IF(@sourceSchema IS NULL)
			SET @sourceSchema = '';
		IF(@sourceObjectName IS NULL)
			SET @sourceObjectName = '';
		IF(@destinationSchema IS NULL)
			SET @destinationSchema = '';
		IF(@destinationObjectName IS NULL)
			SET @destinationObjectName = '';
		IF(@hashKeyColumnName IS NULL)
			SET @hashKeyColumnName = '';
		IF(LEN(@hashKeyColumnName) > 0)
			SET @hashKeyColumnName = '[' + REPLACE(REPLACE(REPLACE(@hashKeyColumnName,']',''),'[',''),',','],[') + ']';
		IF(@debug IS NULL)
			SET @debug = '';
		IF(@loggingType IS NULL)
			SET @loggingType = '';
	
	IF(OBJECT_ID('dbo.BI_log') IS NULL)
		BEGIN
			CREATE TABLE dbo.BI_log
			(
				executionID INT            NOT NULL,
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
			 executionID INT
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
		 @continue            SMALLINT      = 1
		,@sqlScript           NVARCHAR(MAX) = N''
	--LOGGING VARIABLES
		,@executionID         INT           = (SELECT ISNULL(MAX(executionID + 1),1) FROM dbo.BI_log)
		,@execObjectName      VARCHAR(256)  = 'dbo.sp_removeDuplications'
		,@scriptCode          VARCHAR(25)   = ''
		,@status              VARCHAR(50)   = ''
		,@logTreeLevel        TINYINT       = 0
		,@logSpaceTree        VARCHAR(5)    = '    '
		,@message             VARCHAR(500)  = ''
		,@SQL                 VARCHAR(4000) = ''
		,@variables           VARCHAR(2500) = ''
	--GENERAL VARIABLES
		,@sourceObject        NVARCHAR(256) = N''
		,@sourceObjectId      NVARCHAR(256) = N''
		,@destinationObject   NVARCHAR(256) = N''
		,@destinationObjectId NVARCHAR(256) = N'';
	
	SET @sourceObject        = @sourceSchema + '.' + @sourceObjectName;
	SET @sourceObjectId      = OBJECT_ID(@sourceObject);
	SET @destinationObject   = @destinationSchema + '.' + @destinationObjectName;
	SET @destinationObjectId = OBJECT_ID(@destinationObject);
	SET @variables           = ' | @sourceSchema = '          + ISNULL(CONVERT(VARCHAR(128),@sourceSchema         ),'') +
					           ' | @sourceObjectName = '      + ISNULL(CONVERT(VARCHAR(128),@sourceObjectName     ),'') +
					           ' | @destinationSchema = '     + ISNULL(CONVERT(VARCHAR(128),@destinationSchema           ),'') +
					           ' | @destinationObjectName = ' + ISNULL(CONVERT(VARCHAR(128),@destinationObjectName),'') +
					           ' | @hashKeyColumnName = '     + ISNULL(CONVERT(VARCHAR(600),@hashKeyColumnName    ),'') +
					           ' | @debug = '                 + ISNULL(CONVERT(VARCHAR(1)  ,@debug                ),'') +
					           ' | @loggingType = '           + ISNULL(CONVERT(VARCHAR(1)  ,@loggingType          ),'');
			
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
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Input Parameters';
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
	
	--CHECKING INPUT VARIABLES ARE VALID
		IF(SCHEMA_ID(@sourceSchema) IS NULL)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 2;
					SET @scriptCode   = 'COD-100E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Source Schema ' + @sourceSchema + ' does not exists';
					SET @status       = 'ERROR';
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
		ELSE IF(@sourceObjectId IS NULL)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 2;
					SET @scriptCode   = 'COD-200E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Source Object ' + @sourceObject + ' does not exists';
					SET @status       = 'ERROR';
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
		ELSE IF(SCHEMA_ID(@destinationSchema) IS NULL)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 2;
					SET @scriptCode   = 'COD-300E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Destination Schema ' + @destinationSchema + ' does not exists';
					SET @status       = 'ERROR';
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
		ELSE IF(
			@destinationObjectId IS NOT NULL 
			AND EXISTS(
				SELECT 1
				FROM sys.objects a
				WHERE
					a.object_id = @destinationObjectId
					AND a.type <> 'U'
			)
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 2;
					SET @scriptCode   = 'COD-400E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Destination Object ' + @destinationObject + ' is not a table';
					SET @status       = 'ERROR';
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
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The input variables (@sourceSchema | @sourceObjectName | @destinationSchema) are valid';
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
		
	--CHECKING OPTIONAL INPUT VARIABLES
		--VALIDATING COLUMNS
			IF(@continue = 1)
				BEGIN
					IF(@hashKeyColumnName IS NULL OR LEN(RTRIM(LTRIM(@hashKeyColumnName))) = 0)
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 2;
								SET @scriptCode   = 'COD-500E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The parameter @hashKeyColumnName is mandatory';
								SET @status       = 'ERROR';
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
							--PROVIDED BY THE INPUT PARAMETER @hashKeyColumnName
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 2;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Columns for Hash Key provided by an input parameter';
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
											SET @logTreeLevel = 2;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Hash Columns validation';
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
									EXISTS(
										SELECT 1
										FROM
											dbo.udf_DelimitedSplit8K(@hashKeyColumnName,',') a LEFT JOIN sys.columns b ON
												    b.object_id        = @sourceObjectId
												AND '[' + b.name + ']' = a.item
										WHERE 
											    b.object_id IS NULL
											AND a.item <> ''
									)
								)
									BEGIN
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-600E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Hash Column provided in @hashKeyColumnName does not exist in (' + @sourceObject + ')';
											SET @status       = 'ERROR';
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
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Validation Success';
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
											SET @logTreeLevel = 2;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Hash Columns validation';
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
				END

		----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
			IF(@debug = 1)
				BEGIN
					SET @logTreeLevel = 1;
					SET @scriptCode   = '';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Input Variables';
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
	
	--GENERATING UNIQUE ID
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 1;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN TRANSACTION';
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
		
				BEGIN TRANSACTION
				
				IF(@continue = 1)
					BEGIN
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 2;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Generating the Rank Column';
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
							SET @sqlScript = N'EXEC dbo.sp_addRankColumn ''' + @sourceSchema + ''',''' + @sourceObjectName + ''',''BI_HFR'',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-100I';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL Script';
										SET @status       = 'Information';
										SET @SQL          = @sqlScript;
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
										   	RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
							EXEC sp_executesql @sqlScript, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
							
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
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
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
										SET @scriptCode   = 'COD-700E';
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
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-800E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to create the Rank Column';
								SET @status       = 'ERROR';
								SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
								   	RAISERROR(@message,10,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END CATCH
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 2;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Generating the Rank Column';
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
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Re-Generating HashKey';
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
							SET @sqlScript = 'dbo.sp_generateHashKey ''' + @sourceSchema + ''',''' + @sourceObjectName + ''',''' + @destinationSchema + ''',''' + @destinationObjectName + ''','''','''','''',0,3';
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-200I';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL Script';
										SET @status       = 'Information';
										SET @SQL          = @sqlScript;
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
										   	RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
							EXEC(@sqlScript);
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Hash Key created successfully';
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
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-900E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to Re-Generate the Hash Key';
								SET @status       = 'ERROR';
								SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
								   	RAISERROR(@message,10,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END CATCH
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 2;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Re-Generating HashKey';
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
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Dropping RANK_NO Column';
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
								EXISTS(
									SELECT 1
									FROM sys.columns 
									WHERE 
										OBJECT_ID = OBJECT_ID(@sourceSchema + '.' + @sourceObjectName)
										AND name = 'RANK_NO'
								)
							)
								BEGIN
									BEGIN TRY
										SET @sqlScript = 'ALTER TABLE ' + @sourceSchema + '.' + @sourceObjectName + ' DROP COLUMN RANK_NO';
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = 'COD-300I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL Script';
													SET @status       = 'Information';
													SET @SQL          = @sqlScript;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
														END
													IF(@loggingType IN (2,3))
													   	RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										
										EXEC(@sqlScript);
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'RANK_NO column dropped successfully in (' + @sourceSchema + '.' + @sourceObjectName + ')';
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
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1000E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to drop the RANK_NO column in (' + @sourceSchema + '.' + @sourceObjectName + ')';
											SET @status       = 'ERROR';
											SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
											   	RAISERROR(@message,10,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END CATCH
								END
							
							IF(
								EXISTS(
									SELECT 1
									FROM sys.columns 
									WHERE 
										OBJECT_ID = OBJECT_ID(@destinationSchema + '.' + @destinationObjectName)
										AND name = 'RANK_NO'
								)
							)
								BEGIN
									BEGIN TRY
										SET @sqlScript = 'ALTER TABLE ' + @destinationSchema + '.' + @destinationObjectName + ' DROP COLUMN RANK_NO';
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = 'COD-300I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL Script';
													SET @status       = 'Information';
													SET @SQL          = @sqlScript;
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
														END
													IF(@loggingType IN (2,3))
													   	RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										
										EXEC(@sqlScript);
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'RANK_NO column dropped successfully in (' + @destinationSchema + '.' + @destinationObjectName + ')';
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
									END TRY
									BEGIN CATCH
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1100E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to drop the RANK_NO column in (' + @destinationSchema + '.' + @destinationObjectName + ')';
											SET @status       = 'ERROR';
											SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
											   	RAISERROR(@message,10,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END CATCH
								END
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 2;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Dropping RANK_NO Column';
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
									SET @logTreeLevel = 1;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'COMMIT TRANSACTION';
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
						COMMIT TRANSACTION;
					END
				ELSE
					BEGIN
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 1;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'ROLLBACK TRANSACTION';
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
						ROLLBACK TRANSACTION;
					END
			END
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 0;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Store Procedure';
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

	--Inserting Log into the physical table
		INSERT INTO dbo.BI_log
			SELECT * FROM @BI_log;
	
	--RAISE ERROR IN CASE OF
		IF(@continue = 0)
			BEGIN
				DECLARE @errorMessage NVARCHAR(300);
				
				SET @errorMessage = N'PLEASE CHECK --> SELECT * FROM dbo.BI_log WHERE executionID = ' + CONVERT(NVARCHAR(20),@executionID);
				
				RAISERROR(@errorMessage,11,1);
			END 
END
GO
