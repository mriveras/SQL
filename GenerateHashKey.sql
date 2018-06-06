CREATE PROCEDURE [dbo].[sp_generateHashKey] 
	(
		 @sourceSchema          NVARCHAR(128)
		,@sourceObjectName      NVARCHAR(128)
		,@destinationSchema     NVARCHAR(128)
		,@destinationObjectName NVARCHAR(128)
		,@hashKeyColumns        NVARCHAR(MAX) = N''
		,@dateColumn            NVARCHAR(128) = N''
		,@monthsBack            NVARCHAR(3)   = N''
		,@debug                 SMALLINT      = 0
		,@loggingType           SMALLINT      = 1 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
	)
AS
/*
	Developed by: Mauricio Rivera
	Date: 10 May 2018
	
	MODIFICATIONS
		
		
	LAST USED LOGGING IDS:
		- ERRORS      (COD-1400E)
		- INFORMATION (COD-500I)
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
		IF(@hashKeyColumns IS NULL)
			SET @hashKeyColumns = '';
		IF(LEN(@hashKeyColumns) > 0)
			SET @hashKeyColumns = '[' + REPLACE(REPLACE(REPLACE(@hashKeyColumns,']',''),'[',''),',','],[') + ']';
		IF(@dateColumn IS NULL)
			SET @dateColumn = '';
		IF(@monthsBack IS NULL)
			SET @monthsBack = '';
		IF(@debug IS NULL)
			SET @debug = '';
		IF(@loggingType IS NULL)
			SET @loggingType = '';
	
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
		 @continue            SMALLINT        = 1
		,@sqlScript           NVARCHAR(MAX)   = N''
		,@a                   INT             = 0
		,@b                   INT             = 0
		,@column              NVARCHAR(128)   = ''
	--LOGGING VARIABLES
		,@executionID         BIGINT          = NEXT VALUE FOR dbo.sq_BI_log_executionID
		,@execObjectName      VARCHAR(256)    = 'dbo.sp_generateHashKey'
		,@scriptCode          VARCHAR(25)     = ''
		,@status              VARCHAR(50)     = ''
		,@logTreeLevel        TINYINT         = 0
		,@logSpaceTree        VARCHAR(5)      = '    '
		,@message             VARCHAR(500)    = ''
		,@SQL                 VARCHAR(4000)   = ''
		,@variables           VARCHAR(2500)   = ''
	--GENERAL VARIABLES
		,@sourceObject        NVARCHAR(256)   = @sourceSchema + N'.' + @sourceObjectName
		,@sourceObjectId      INT             = 0
		,@destinationObject   NVARCHAR(256)   = @destinationSchema + N'.' + @destinationObjectName
		,@destinationObjectId INT             = 0
		,@destinationTempHash NVARCHAR(256)   = ''
	
	SET @sourceObjectId      = OBJECT_ID(@sourceObject);
	SET @destinationObjectId = OBJECT_ID(@destinationObject);
	SET @destinationTempHash = @destinationObject + N'_THK';
	SET @variables           = ' | @sourceSchema = '          + ISNULL(CONVERT(VARCHAR(128),@sourceSchema         ),'') +
					           ' | @sourceObjectName = '      + ISNULL(CONVERT(VARCHAR(128),@sourceObjectName     ),'') +
					           ' | @destinationSchema = '     + ISNULL(CONVERT(VARCHAR(128),@dateColumn           ),'') +
					           ' | @destinationObjectName = ' + ISNULL(CONVERT(VARCHAR(128),@destinationObjectName),'') +
					           ' | @hashKeyColumns = '        + ISNULL(CONVERT(VARCHAR(600),@hashKeyColumns       ),'') +
					           ' | @dateColumn = '            + ISNULL(CONVERT(VARCHAR(128),@dateColumn           ),'') +
					           ' | @monthsBack = '            + ISNULL(CONVERT(VARCHAR(3)  ,@monthsBack           ),'') +
					           ' | @debug = '                 + ISNULL(CONVERT(VARCHAR(1)  ,@debug                ),'') +
					           ' | @loggingType = '           + ISNULL(CONVERT(VARCHAR(1)  ,@loggingType          ),'') +
					           ' | @sourceObjectId = '        + ISNULL(CONVERT(VARCHAR(10) ,@sourceObjectId       ),'') +
					           ' | @sourceObject = '          + ISNULL(CONVERT(VARCHAR(256),@sourceObject         ),'') +
					           ' | @destinationObjectId = '   + ISNULL(CONVERT(VARCHAR(10) ,@destinationObjectId  ),'') +
					           ' | @destinationObject = '     + ISNULL(CONVERT(VARCHAR(256),@destinationObject    ),'') +
					           ' | @destinationTempHash = '   + ISNULL(CONVERT(VARCHAR(256),@destinationTempHash  ),'');
			
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
					IF(@hashKeyColumns IS NOT NULL AND LEN(RTRIM(LTRIM(@hashKeyColumns))) > 0)
						BEGIN 
							--PROVIDED BY THE INPUT PARAMETER @hashKeyColumns
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
											dbo.udf_DelimitedSplit8K(@hashKeyColumns,',') a LEFT JOIN sys.columns b ON
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
											SET @scriptCode   = 'COD-500E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Hash Columns provided in @hashKeyColumns does not exist in (' + @sourceObject + ')';
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
					ELSE
						BEGIN
							--IF NO COLUMNS ARE SPECIFIED BY THE INPUT VARIABLE, ALL COLUMNS OF THE OBJECT ARE SET
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 2;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Columns for Hash Key NOT provided by an input parameter';
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
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN automatic assignation of columns for the Hash Key';
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
								
								SET @hashKeyColumns = (
									SELECT
										STUFF(
											(
											SELECT ',[' + name + ']'
											FROM sys.columns
											WHERE
												    object_id = @sourceObjectId
												AND name NOT IN ('ProcessExecutionID','LoadDateTime','BI_HFR','BI_HFR_V1')
											FOR XML PATH(''), TYPE
											).value('.', 'VARCHAR(MAX)'), 1, 1, ''
										)
								);
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Columns assigned: ' + @hashKeyColumns;
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
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END automatic assignation of columns for the Hash Key';
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
		
		--VALIDATING THE EXISTENCE OF @monthsBack AND @dateColumn AND VICE VERSA IF ONE IS SPECIFIED
			IF(LEN(RTRIM(LTRIM(@monthsBack))) > 0 OR LEN(RTRIM(LTRIM(@dateColumn))) > 0)
				BEGIN
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Input Parameters @dateColumn and @monthsBack has data';
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
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN validation of @dateColumn and @monthsBack';
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
								
					IF(LEN(RTRIM(LTRIM(@monthsBack))) = 0)
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-600E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'If input variable @dateColumn is specified, the imput variable @monthsBack is required';
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
					ELSE IF(LEN(RTRIM(LTRIM(@dateColumn))) = 0)
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-700E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'If input variable @monthsBack is specified, the imput variable @dateColumn is required';
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
				END
		
		--VALIDATING DATE COLUMN (If specified)
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
							   	    a.OBJECT_ID = @sourceObjectId
								AND a.name      = @dateColumn
								AND b.name     IN ('datetime','smalldatetime','datetime2')
						)
					)
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-800E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Date Column ' + @dateColumn + ' does not exists or has not valid DateTime data tyle in the Source Object ' + @sourceObject;
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
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The input parameter @dateColumn is valid';
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
		
		--VALIDATING MONTH BACK (If specified)
			IF(@continue = 1 AND LEN(RTRIM(LTRIM(@dateColumn))) > 0)
				BEGIN
					IF(ISNUMERIC(@monthsBack) = 1 AND CONVERT(INT,@monthsBack) < 1)
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-900E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The input variable @monthsBack has a wrong value. It should be numeric and greater than zero (0)';
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
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The input parameter @monthsBack is valid';
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
			
			IF(LEN(RTRIM(LTRIM(@monthsBack))) > 0 OR LEN(RTRIM(LTRIM(@dateColumn))) > 0)
				BEGIN
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END validation of @dateColumn and @monthsBack';
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
	
	--GENERATING THE HASH KEY
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 1;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Generating Hash Key tables';
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
				
				--DROPPING TEMP TABLE
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN dropping temp hash table';
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
							IF(OBJECT_ID(@destinationTempHash) IS NOT NULL)
								BEGIN
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Destination Temp Hash Table (' + @destinationTempHash + ') found';
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
									
									SET @sqlScript = 'DROP TABLE ' + @destinationTempHash;
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
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
									
									EXEC(@sqlScript);
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Hash Table dropped sucessfully';
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
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Hash Table (' + @destinationTempHash + ') not found';
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
							SET @logTreeLevel = 4;
							SET @scriptCode   = 'COD-1000E';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to drop Temp Hash Table (' + @destinationTempHash + ')';
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
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END dropping temp hash table';
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

				
				--GENERATING TEMP TABLE WITH HASHKEY
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Generating Temp Hash Table';
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
								
									SET @sqlScript = 						N'SELECT ';
									IF(
										EXISTS(
											SELECT 1 FROM sys.columns WHERE object_id = @sourceObjectId AND name IN ('ProcessExecutionID','LoadDateTime')
										)
									)
										BEGIN
											SET @sqlScript = @sqlScript + 		N'
	 LoadDateTime
	,ProcessExecutionID,';
										END
											SET @sqlScript = @sqlScript + 		N'
	CONVERT(
		 VARCHAR(40)
		,HASHBYTES(
			''SHA2_512'' 
			,UPPER( ' +
																							STUFF(
																								( 
																									SELECT 
																										 
				N' + ' + 
																										CASE
																											WHEN c.[precision] = 0 THEN --Data Types Strings and TimeStamp
																												CASE 
																													WHEN (UPPER(c.name) = 'TIMESTAMP') THEN NULL --NULL is to exclude TimeStamp columns
																													WHEN (UPPER(c.name) = 'UNIQUEIDENTIFIER') THEN
'
				ISNULL(CONVERT(VARCHAR(36),a.[' + b.name + ']),''¿'') + ''±''' --UniqueIdentifier Columns
																													WHEN (UPPER(c.name) IN ('NVARCHAR','NCHAR','NTEXT')) THEN
'
				RTRIM(LTRIM(ISNULL(CONVERT(VARCHAR(' + CONVERT(NVARCHAR(10),b.max_length / 2) + '),a.[' + b.name + ']),''¿''))) + ''±''' --Nvarchar, Nchar and Ntext Columns
																													ELSE 
'
				RTRIM(LTRIM(ISNULL(CONVERT(VARCHAR(' + CONVERT(NVARCHAR(10),b.max_length) + '),a.[' + b.name + ']),''¿''))) + ''±''' --String Columns
																												END
																											ELSE --Data Types Non-Strings (such as Numeric,Decimal,INT,FLOAT,...)
																												CASE 
																													WHEN (UPPER(c.name) IN ('FLOAT','REAL','NUMERIC','DECIMAL')) THEN 
'
				ISNULL(CONVERT(VARCHAR(' + CONVERT(NVARCHAR(10),c.[precision] + 2) + '),a.[' + b.name + '],3),''¿'') + ''±''' --Float or Real Columns
																													WHEN (UPPER(c.name) IN ('MONEY','SMALLMONEY')) THEN
'
				ISNULL(CONVERT(VARCHAR(' + CONVERT(NVARCHAR(10),c.[precision] + 2) + '),a.[' + b.name + '],2),''¿'') + ''±''' --Money or SmallMoney Columns
																													WHEN (UPPER(c.name) = 'DATETIME2') THEN
'
				ISNULL(CONVERT(VARCHAR(27),a.[' + b.name + '],121),''¿'') + ''±''' --Datetime2 Columns
																													WHEN (UPPER(c.name) = 'DATETIME') THEN
'
				ISNULL(CONVERT(VARCHAR(19),a.[' + b.name + '],120),''¿'') + ''±''' --Datetime Columns
																													WHEN (UPPER(c.name) = 'SMALLDATETIME') THEN
'
				ISNULL(CONVERT(VARCHAR(19),a.[' + b.name + '],100),''¿'') + ''±''' --SmallDateTime Columns
																													WHEN (UPPER(c.name) = 'DATE') THEN
'
				ISNULL(CONVERT(VARCHAR(10),a.[' + b.name + '],103),''¿'') + ''±''' --Date Columns
																													ELSE
'
				ISNULL(CONVERT(VARCHAR(' + CONVERT(NVARCHAR(10),c.[precision] + 2) + '),a.[' + b.name + ']),''¿'') + ''±''' --All other Non-String Columns
																												END
																										END
																									FROM 
																										sys.objects a INNER JOIN sys.columns b ON
																											    b.object_id = a.object_id
																										INNER JOIN sys.types c ON
																											    c.system_type_id = b.system_type_id
																											AND c.user_type_id   = b.user_type_id
																									WHERE
																										    '[' + b.name + ']' IN (SELECT Item FROM dbo.udf_DelimitedSplit8K(@hashKeyColumns,','))
																										 AND a.object_id = @sourceObjectId
																									ORDER BY b.name ASC
																									FOR XML PATH(''), TYPE
																								).value('.', 'VARCHAR(MAX)'), 1, 3, ''
																							) 
				+ N'
			)
		), 2
	) AS BI_HFR
' 
																				+ STUFF(
																					( 
																						SELECT  
																							CASE
																								WHEN c.[precision] = 0 THEN --Data Types Strings and TimeStamp
																									CASE
																										WHEN (UPPER(c.name) = 'TIMESTAMP') THEN
'	,CONVERT(' + UPPER(c.name) +  ',a.[' + b.name + ']) AS [' + b.name + ']' --TimeStamp Columns	
																										WHEN (UPPER(c.name) IN ('NVARCHAR','NCHAR','NTEXT')) THEN
'	,CONVERT(' + UPPER(c.name) +  '(' + CONVERT(NVARCHAR(10),b.max_length / 2) + '),a.[' + b.name + ']) AS [' + b.name + ']' --Nvarchar, Nchar and Ntext Columns 
																										ELSE 
'	,CONVERT(' + UPPER(c.name) +  '(' + CONVERT(NVARCHAR(10),b.max_length) + '),a.[' + b.name + ']) AS [' + b.name + ']' --String Columns 
																									END
																								ELSE --Data Types Non-Strings (such as Decimal,INT,FLOAT,...)
																									CASE
																										WHEN (UPPER(c.name) IN ('NUMERIC','DECIMAL')) THEN 
'	,CONVERT(' + UPPER(c.name) +  '(' + CONVERT(NVARCHAR(10),b.precision) + ',' + CONVERT(NVARCHAR(10),b.scale) + '),a.[' + b.name + ']) AS [' + b.name + ']' --Non-Strings Columns
																										ELSE
'	,CONVERT(' + UPPER(c.name) +  ',a.[' + b.name + ']) AS [' + b.name + ']' --Non-Strings Columns
																									END						
																							END + N'
'
																						FROM 
																							sys.objects a INNER JOIN sys.columns b ON
																								    b.object_id = a.object_id
																							INNER JOIN sys.types c ON
																								    c.system_type_id = b.system_type_id
																								AND c.user_type_id   = b.user_type_id
																						WHERE
																							     b.name NOT IN ('BI_HFR','LoadDateTime','ProcessExecutionID','uniqueID')
																							 AND a.object_id = @sourceObjectId
																						ORDER BY
																							b.column_id ASC
																						FOR XML PATH(''), TYPE
																					).value('.', 'VARCHAR(MAX)'), 1, 0, ''
																				) + 
N'INTO ' + @destinationTempHash + N'
FROM ' + @sourceObject + N' a';
									IF(LEN(RTRIM(LTRIM(@dateColumn))) > 0)
										BEGIN
											SET @sqlScript = @sqlScript + 	N'
WHERE ';
											SET @sqlScript = @sqlScript +	N'
	' + @dateColumn + N' >= DATEADD(MONTH,-' + @monthsBack + N',GETDATE())';
										END	
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 4;
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
											SET @logTreeLevel = 4;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Hash Table (' + @destinationTempHash + ') created sucessfully';
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
									SET @logTreeLevel = 4;
									SET @scriptCode   = 'COD-1100E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to create Temp Hash Table (' + @destinationTempHash + ')';
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
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Generating Temp Hash Table';
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
				
				--DROPPING DESTINATION TABLE
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN dropping Destination table';
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
									IF(OBJECT_ID(@destinationObject) IS NOT NULL)
										BEGIN
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 4;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Destination Table (' + @destinationObject +  ') found';
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
		
											SET @sqlScript = 'DROP TABLE ' + @destinationObject;
											
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 4;
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
														SET @logTreeLevel = 4;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Destination Table (' + @destinationObject + ') dropped successfully';
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
								---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 4;
									SET @scriptCode   = 'COD-1200E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to drop Destination Table (' + @destinationObject + ')';
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
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END dropping Destination table';
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
					
				--GENERATING DESTINATION TABLE FROM TEMP TABLE
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN generating Destination Table';
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
									SET @sqlScript = 	'SELECT * INTO ' + @destinationObject + ' FROM ' + @destinationTempHash;
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = 'COD-400I';
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
											
									EXEC(@sqlScript)
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Destination table (' + @destinationObject + ') generated successfully';
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
								---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 4;
									SET @scriptCode   = 'COD-1300E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to generate the Destination Table (' + @destinationObject + ')';
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
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END generating Destination Table';
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
				
				--DROPPING TEMP TABLE
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN dropping Temp Hash Table';
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
									IF(OBJECT_ID(@destinationTempHash) IS NOT NULL)
										BEGIN
											SET @sqlScript = 'DROP TABLE ' + @destinationTempHash;
											
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 4;
														SET @scriptCode   = 'COD-500I';
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
														SET @logTreeLevel = 4;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Temp Hash Table (' + @destinationTempHash + ') dropped successfully';
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
								---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 4;
									SET @scriptCode   = 'COD-1400E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Error while trying to drop Temp Hash Table (' + @destinationObject + ')';
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
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END dropping Temp Hash Table';
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
									SET @logTreeLevel = 2;
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
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 1;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Generating Hash Key tables';
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

