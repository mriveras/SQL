CREATE PROCEDURE [dbo].[sp_generateHashKey] 
	(
		 @sourceSchema          NVARCHAR(128)
		,@sourceObjectName      NVARCHAR(128)
		,@destinationSchema     NVARCHAR(128)
		,@destinationObjectName NVARCHAR(128)
		,@hashKeyColumns        NVARCHAR(MAX) = ''
		,@dateColumn            NVARCHAR(128) = ''
		,@monthsBack            NVARCHAR(3)   = ''
		,@debug                 SMALLINT      = 0
		,@loggingType           SMALLINT      = 1 --1) Table | 2) Console | 3) Table & Console
	)
AS
/*
  Developer: Mauricio Rivera Senior
  Date: 15 Feb 2018
  
  Description:
    This SP generates a new column with a hash key. If the source and destination are the same, the hash key will be created in the
    source table. The parameter @hashKeyColumns allows indicating which columns will be considered for the generation of the hash
    key. If an incremental process is required, the parameter @dateColumn will be used to specify the name of the date column in the
    source object. As well, @monthsBack is mandatory and set the number of months that the process will filter the source to generate
    the hash key. The @debug parameter allows registering more information about the execution. This parameter is particularly useful
    for debugging. @loggingType specifies the target of the logging; it will be stored in a table, shown in the prompt or both.
*/
BEGIN
	DECLARE 
		 @continue            SMALLINT
		,@executionID         INT
		,@message             NVARCHAR(1000)
		,@sourceObjectId      INT
		,@sourceObject        NVARCHAR(256)
		,@destinationObjectId INT
		,@destinationObject   NVARCHAR(256)
		,@destinationTempHash NVARCHAR(256)
		,@column              NVARCHAR(128)
		,@a                   INT
		,@b                   INT
		,@sqlScript           NVARCHAR(MAX)
		,@logTreeLevel        INT
		,@scriptCode          NVARCHAR(10)
		,@logType             NVARCHAR(100)
		,@logProcess          NVARCHAR(MAX)
		,@logScript           NVARCHAR(MAX)
		,@logSpaceTree        NVARCHAR(5);
	
	SET @continue            = 1;
	SET @sourceObject        = @sourceSchema + N'.' + @sourceObjectName;
	SET @destinationObject   = @destinationSchema + N'.' + @destinationObjectName;
	SET @sourceObjectId      = OBJECT_ID(@sourceObject);
	SET @destinationObjectId = OBJECT_ID(@destinationObject);
	SET @destinationTempHash = @destinationObject + N'_THK';
	SET @logSpaceTree        = N'    ';
	
	--CHECKING IF LOG TABLE EXIST
		IF(OBJECT_ID('dbo.generateHashKey_log') IS NULL)
			BEGIN
				--CREATING LOG TABLE
					CREATE TABLE dbo.generateHashKey_log
						(
							executionID           INT NOT NULL,
							sequenceID            INT NOT NULL,
							executionDateTime     DATETIME NOT NULL,
							sourceSchema          NVARCHAR (128) NOT NULL,
							sourceObjectName      NVARCHAR (128) NOT NULL,
							destinationSchema     NVARCHAR (128) NOT NULL,
							destinationObjectName NVARCHAR (128) NOT NULL,
							hashKeyColumns        NVARCHAR (max) NOT NULL,
							dateColumn            NVARCHAR (128) NOT NULL,
							monthsBack            NVARCHAR (3) NOT NULL,
							scriptCode            NVARCHAR (19) NOT NULL,
							logType               NVARCHAR (100) NOT NULL,
							logProcess            NVARCHAR (max) NOT NULL,
							logScript             NVARCHAR (max) NOT NULL,
							CONSTRAINT PK_generateHashKey_log PRIMARY KEY (executionID, sequenceID)
						);
			END
	
	--GETTING THE EXECUTION ID
		SELECT @executionID = ISNULL(MAX(executionID + 1),1)
		FROM dbo.generateHashKey_log
	
	--CREATING LOG TABLE VARIABLE
		DECLARE @generateHashKey_log TABLE (
			 executionID           INT
			,sequenceID            INT IDENTITY(1,1)
			,executionDateTime     DATETIME
			,sourceSchema          NVARCHAR (128)
			,sourceObjectName      NVARCHAR (128)
			,destinationSchema     NVARCHAR (128)
			,destinationObjectName NVARCHAR (128)
			,hashKeyColumns        NVARCHAR (max)
			,dateColumn            NVARCHAR (128)
			,monthsBack            NVARCHAR (3)
			,scriptCode            NVARCHAR (10)
			,logType               NVARCHAR (100)
			,logProcess            NVARCHAR (MAX)
			,logScript             NVARCHAR (MAX)
		)
			
	--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 0;
				SET @scriptCode   = N'COD-0';
				SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN Store Procedure';
				SET @logType      = N'Information';
				SET @logScript    = N'';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
						VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@logProcess,10,1);
			END
	--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
	
	--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 1;
				SET @scriptCode   = N'COD-100';
				SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN Checking Input Variables';
				SET @logType      = N'Information';
				SET @logScript    = N'';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
						VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@logProcess,10,1);
			END
	--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
	
	--CHECKING INPUT VARIABLES ARE VALID
		IF(SCHEMA_ID(@sourceSchema) IS NULL)
			BEGIN
				SET @continue = 0;
				--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
					SET @logTreeLevel = 2;
					SET @scriptCode   = N'COD-200';
					SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'The Source Schema ' + @sourceSchema + ' does not exists';
					SET @logType      = N'ERROR';
					SET @logScript    = N'';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
							VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@logProcess,11,1);
				--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
			END
		ELSE IF(@sourceObjectId IS NULL)
			BEGIN
				SET @continue = 0;
				--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
					SET @logTreeLevel = 2;
					SET @scriptCode   = N'COD-300';
					SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'The Source Object ' + @sourceObject + ' does not exists';
					SET @logType      = N'ERROR';
					SET @logScript    = N'';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
							VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@logProcess,11,1);
				--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
			END
		ELSE IF(SCHEMA_ID(@destinationSchema) IS NULL)
			BEGIN
				SET @continue = 0;
				--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
					SET @logTreeLevel = 2;
					SET @scriptCode   = N'COD-400';
					SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'The Destination Schema ' + @destinationSchema + ' does not exists';
					SET @logType      = N'ERROR';
					SET @logScript    = N'';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
							VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@logProcess,11,1);
				--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
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
				--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
					SET @logTreeLevel = 2;
					SET @scriptCode   = N'COD-500';
					SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'The Destination Object ' + @destinationObject + ' is not a table.';
					SET @logType      = N'ERROR';
					SET @logScript    = N'';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
							VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@logProcess,11,1);
				--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
			END
		ELSE
			BEGIN
				--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = N'COD-600';
							SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'The input variables (@sourceSchema | @sourceObjectName | @destinationSchema) are valid';
							SET @logType      = N'Information';
							SET @logScript    = N'';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
									VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@logProcess,10,1);
						END
				--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
			END
		
		
			
	--CHECKING OPTIONAL INPUT VARIABLES
		--VALIDATING COLUMNS
			IF(@continue = 1)
				BEGIN
					IF(@hashKeyColumns IS NOT NULL AND LEN(RTRIM(LTRIM(@hashKeyColumns))) > 0)
						BEGIN 
							--PROVIDED BY THE INPUT PARAMETER @hashKeyColumns
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									IF(@debug = 1)
										BEGIN 
											SET @logTreeLevel = 2;
											SET @scriptCode   = N'COD-700';
											SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Columns for Hash Key provided by an input parameter';
											SET @logType      = N'Information';
											SET @logScript    = N'';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
													VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@logProcess,10,1);
										END
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
								
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									IF(@debug = 1)
										BEGIN 
											SET @logTreeLevel = 2;
											SET @scriptCode   = N'COD-800';
											SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN Columns validation';
											SET @logType      = N'Information';
											SET @logScript    = N'';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
													VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@logProcess,10,1);
										END
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
														
								SET @a = 1;
								SET @b = 0;
								
								WHILE (@continue = 1 AND @a <= LEN(@hashKeyColumns))
									BEGIN
										SET @b = CHARINDEX(',',@hashKeyColumns,@a) - @a;
										IF(@b <= 0)
											BEGIN
												SET @b = LEN(@hashKeyColumns) - @a + 1;
											END
										SET @column = SUBSTRING(@hashKeyColumns,@a,@b);
										SET @a = @a + @b + 1;
										
										IF(
											NOT EXISTS(
												SELECT 1
												FROM sys.columns
												WHERE
													    object_id = @sourceObjectId
													AND name      = LTRIM(RTRIM(@column))
											)
										)
											BEGIN
												SET @continue = 0;
												--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
													SET @logTreeLevel = 3;
													SET @scriptCode   = N'COD-900';
													SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Column ' + @column + ' NOT FOUND in the Source Object ' + @sourceObject;
													SET @logType      = N'ERROR';
													SET @logScript    = N'';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
															VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@logProcess,11,1);
												--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
											END
										ELSE
											BEGIN
												--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = 3;
															SET @scriptCode   = N'COD-1000';
															SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Column ' + @column + ' found in the Source Object ' + @sourceObject;
															SET @logType      = N'Information';
															SET @logScript    = N'';
															IF(@loggingType IN (1,3))
																BEGIN
																	INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
																	VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
																END
															IF(@loggingType IN (2,3))
																RAISERROR(@logProcess,10,1);
														END
												--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
											END 
									END
								
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									IF(@debug = 1)
										BEGIN 
											SET @logTreeLevel = 2;
											SET @scriptCode   = N'COD-1100';
											SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END Columns validation';
											SET @logType      = N'Information';
											SET @logScript    = N'';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
													VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@logProcess,10,1);
										END
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END 
					ELSE
						BEGIN
							--IF NO COLUMNS ARE SPECIFIED BY THE INPUT VARIABLE, ALL COLUMNS OF THE OBJECT ARE SET
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									IF(@debug = 1)
										BEGIN 
											SET @logTreeLevel = 2;
											SET @scriptCode   = N'COD-1200';
											SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Columns for Hash Key NOT provided by an input parameter';
											SET @logType      = N'Information';
											SET @logScript    = N'';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
													VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@logProcess,10,1);
										END
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
								
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									IF(@debug = 1)
										BEGIN 
											SET @logTreeLevel = 2;
											SET @scriptCode   = N'COD-1300';
											SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN automatic assignation of columns for the Hash Key';
											SET @logType      = N'Information';
											SET @logScript    = N'';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
													VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@logProcess,10,1);
										END
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
								
								SELECT 
									@hashKeyColumns = STUFF(
										(
										SELECT ',' + name
										FROM sys.columns
										WHERE
											    object_id = @sourceObjectId
											AND name NOT IN ('ProcessExecutionID','LoadDateTime')
										FOR XML PATH(''), TYPE
										).value('.', 'VARCHAR(MAX)'), 1, 1, ''
									)
								
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									IF(@debug = 1)
										BEGIN 
											SET @logTreeLevel = 3;
											SET @scriptCode   = N'COD-1400';
											SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Columns assigned: ' + @hashKeyColumns;
											SET @logType      = N'Information';
											SET @logScript    = N'';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
													VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@logProcess,10,1);
										END
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
								
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									IF(@debug = 1)
										BEGIN 
											SET @logTreeLevel = 2;
											SET @scriptCode   = N'COD-1500';
											SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END automatic assignation of columns for the Hash Key';
											SET @logType      = N'Information';
											SET @logScript    = N'';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
													VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@logProcess,10,1);
										END
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
				END
		
		--VALIDATING THE EXISTENCE OF @monthsBack AND @dateColumn AND VICE VERSA IF ONE IS SPECIFIED
			IF(LEN(RTRIM(LTRIM(@monthsBack))) > 0 OR LEN(RTRIM(LTRIM(@dateColumn))) > 0)
				BEGIN
					--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
						IF(@debug = 1)
							BEGIN 
								SET @logTreeLevel = 2;
								SET @scriptCode   = N'COD-1600';
								SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Input Parameters @dateColumn and @monthsBack has data';
								SET @logType      = N'Information';
								SET @logScript    = N'';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
										VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@logProcess,10,1);
							END
					--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
					
					--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
						IF(@debug = 1)
							BEGIN 
								SET @logTreeLevel = 2;
								SET @scriptCode   = N'COD-1700';
								SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN validation of @dateColumn and @monthsBack';
								SET @logType      = N'Information';
								SET @logScript    = N'';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
										VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@logProcess,10,1);
							END
					--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
								
					IF(LEN(RTRIM(LTRIM(@monthsBack))) = 0)
						BEGIN
							SET @continue = 0;
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								SET @logTreeLevel = 3;
								SET @scriptCode   = N'COD-1800';
								SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'If input variable @dateColumn is specified, the imput variable @monthsBack is required.';
								SET @logType      = N'ERROR';
								SET @logScript    = N'';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
										VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@logProcess,11,1);
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
					ELSE IF(LEN(RTRIM(LTRIM(@dateColumn))) = 0)
						BEGIN
							SET @continue = 0;
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								SET @logTreeLevel = 3;
								SET @scriptCode   = N'COD-1900';
								SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'If input variable @monthsBack is specified, the imput variable @dateColumn is required.';
								SET @logType      = N'ERROR';
								SET @logScript    = N'';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
										VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@logProcess,11,1);
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
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
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								SET @logTreeLevel = 3;
								SET @scriptCode   = N'COD-2000';
								SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'The Date Column ' + @dateColumn + N' does not exists or has not valid DateTime data tyle in the Source Object ' + @sourceObject;
								SET @logType      = N'ERROR';
								SET @logScript    = N'';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
										VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@logProcess,11,1);
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
					ELSE
						BEGIN
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN 
										SET @logTreeLevel = 3;
										SET @scriptCode   = N'COD-2100';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'The input parameter @dateColumn is valid';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
				END
		
		--VALIDATING MONTH BACK (If specified)
			IF(@continue = 1 AND LEN(RTRIM(LTRIM(@dateColumn))) > 0)
				BEGIN
					IF(ISNUMERIC(@monthsBack) = 1 AND CONVERT(INT,@monthsBack) < 1)
						BEGIN
							SET @continue = 0;
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								SET @logTreeLevel = 3;
								SET @scriptCode   = N'COD-2200';
								SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'The input variable @monthsBack has a wrong value. It should be numeric and greater than zero (0).';
								SET @logType      = N'ERROR';
								SET @logScript    = N'';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
										VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@logProcess,11,1);
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
					ELSE
						BEGIN
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN 
										SET @logTreeLevel = 2;
										SET @scriptCode   = N'COD-2300';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'The input parameter @monthsBack is valid';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
				END
			
			IF(LEN(RTRIM(LTRIM(@monthsBack))) > 0 OR LEN(RTRIM(LTRIM(@dateColumn))) > 0)
				BEGIN
					--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
						IF(@debug = 1)
							BEGIN 
								SET @logTreeLevel = 2;
								SET @scriptCode   = N'COD-2400';
								SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END validation of @dateColumn and @monthsBack';
								SET @logType      = N'Information';
								SET @logScript    = N'';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
										VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@logProcess,10,1);
							END
					--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
				END
		
		--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
			IF(@debug = 1)
				BEGIN
					SET @logTreeLevel = 1;
					SET @scriptCode   = N'COD-2500';
					SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END Checking Input Variables';
					SET @logType      = N'Information';
					SET @logScript    = N'';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
							VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@logProcess,10,1);
				END
		--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
	
	--GENERATING THE HASH KEY
		IF(@continue = 1)
			BEGIN
				--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 1;
							SET @scriptCode   = N'COD-2600';
							SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN Generating Hash Key tables';
							SET @logType      = N'Information';
							SET @logScript    = N'';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
									VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@logProcess,10,1);
						END
				--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
				
				--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = N'COD-2700';
							SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN TRANSACTION';
							SET @logType      = N'Information';
							SET @logScript    = N'';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
									VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@logProcess,10,1);
						END
				--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
		
				BEGIN TRANSACTION
				
				--DROPPING TEMP TABLE
					--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = N'COD-2800';
								SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN dropping temp hash table';
								SET @logType      = N'Information';
								SET @logScript    = N'';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
										VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@logProcess,10,1);
							END 
					--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
					BEGIN TRY
							IF(OBJECT_ID(@destinationTempHash) IS NOT NULL)
								BEGIN
									--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = N'COD-2900';
												SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Destination Temp Hash Table (' + @destinationTempHash + ') found';
												SET @logType      = N'Information';
												SET @logScript    = N'';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
														VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@logProcess,10,1);
											END 
									--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
									
									SET @sqlScript = 'DROP TABLE ' + @destinationTempHash;
									
									--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = N'COD-3000';
												SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Executing SQL Script';
												SET @logType      = N'Information';
												SET @logScript    = @sqlScript;
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
														VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@logProcess,10,1);
											END 
									--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
									
									EXEC(@sqlScript);
									
									--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = N'COD-3100';
												SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Temp Hash Table dropped sucessfully';
												SET @logType      = N'Information';
												SET @logScript    = N'';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
														VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@logProcess,10,1);
											END 
									--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
								END
							ELSE
								BEGIN
									--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = N'COD-3200';
												SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Temp Hash Table (' + @destinationTempHash + ') not found';
												SET @logType      = N'Information';
												SET @logScript    = N'';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
														VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@logProcess,10,1);
											END 
									--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
								END
					END TRY
					BEGIN CATCH
						SET @continue = 0;
						--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
							SET @logTreeLevel = 4;
							SET @scriptCode   = N'COD-3300';
							SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Error while trying to drop Temp Hash Table (' + @destinationTempHash + ')';
							SET @logType      = N'ERROR';
							SET @logScript    = N'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),N'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),N'') + N') - '+ ISNULL(ERROR_MESSAGE(),N'');
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
									VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@logProcess,11,1);
						--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
					END CATCH
					--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = N'COD-3400';
								SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END dropping temp hash table';
								SET @logType      = N'Information';
								SET @logScript    = N'';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
										VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@logProcess,10,1);
							END 
					--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>

				
				--GENERATING TEMP TABLE WITH HASHKEY
					IF(@continue = 1)
						BEGIN
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = N'COD-3500';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN Generating Temp Hash Table';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END 
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							BEGIN TRY
								
									SET @sqlScript = 						N'SELECT ';
									IF(
										EXISTS(
											SELECT 1 FROM sys.columns WHERE object_id = @sourceObjectId AND name IN ('ProcessExecutionID','LoadDateTime')
										)
									)
										BEGIN
											SET @sqlScript = @sqlScript + 		N'LoadDateTime,
																				ProcessExecutionID, ';
										END
											SET @sqlScript = @sqlScript + 		N'CONVERT(VARCHAR(40)
																					,HASHBYTES(
																						''SHA2_512'' 
																						,UPPER( ' +
																							STUFF(
																								( 
																									SELECT 
																										DISTINCT 
																										CASE
																											WHEN c.[precision] = 0 THEN --Data Types Strings and TimeStamp
																												CASE 
																													WHEN (c.name = 'TIMESTAMP') THEN NULL --NULL is to exclude TimeStamp columns
																													ELSE ' + ISNULL(CONVERT(VARCHAR(' + CONVERT(NVARCHAR(10),b.max_length) + '),a.[' + b.name + ']),''¿'') + ''±''' --String Columns
																												END
																											ELSE --Data Types Non-Strings (such as Decimal,INT,FLOAT,...)
																												CASE 
																													WHEN (c.name IN ('FLOAT','REAL')) THEN 
																														' + ISNULL(CONVERT(VARCHAR(' + CONVERT(NVARCHAR(10),c.[precision] + 2) + '),a.[' + b.name + '],3),''¿'') + ''±''' --Float or Real Columns
																													WHEN (c.name IN ('MONEY','SMALLMONEY')) THEN
																														' + ISNULL(CONVERT(VARCHAR(' + CONVERT(NVARCHAR(10),c.[precision] + 2) + '),a.[' + b.name + '],2),''¿'') + ''±''' --Money or SmallMoney Columns
																													WHEN (c.name = 'DATETIME2') THEN
																														' + ISNULL(CONVERT(VARCHAR(27),a.[' + b.name + '],121),''¿'') + ''±''' --Datetime2 Columns
																													WHEN (c.name = 'DATETIME') THEN
																														' + ISNULL(CONVERT(VARCHAR(19),a.[' + b.name + '],120),''¿'') + ''±''' --Datetime Columns
																													WHEN (c.name = 'SMALLDATETIME') THEN
																														' + ISNULL(CONVERT(VARCHAR(19),a.[' + b.name + '],100),''¿'') + ''±''' --SmallDateTime Columns
																													WHEN (c.name = 'DATE') THEN
																														' + ISNULL(CONVERT(VARCHAR(10),a.[' + b.name + '],103),''¿'') + ''±''' --Date Columns
																													ELSE
																														' + ISNULL(CONVERT(VARCHAR(' + CONVERT(NVARCHAR(10),c.[precision] + 2) + '),a.[' + b.name + ']),''¿'') + ''±''' --All other Non-String Columns
																												END
																										END
																									FROM 
																										sys.objects a INNER JOIN sys.columns b ON
																											    b.object_id = a.object_id
																										INNER JOIN sys.types c ON
																											    c.system_type_id = b.system_type_id
																											AND c.user_type_id   = b.user_type_id
																									WHERE
																										    b.name IN (SELECT Item FROM dbo.udf_DelimitedSplit8K(@hashKeyColumns,','))
																										 AND a.object_id = @sourceObjectId
																									FOR XML PATH(''), TYPE
																								).value('.', 'VARCHAR(MAX)'), 1, 3, ''
																							) 
																						+ N')
																					), 2
																				) AS BI_HFR ' 
																				+ STUFF(
																					( 
																						SELECT 
																							',' + b.name
																						FROM 
																							sys.objects a INNER JOIN sys.columns b ON
																								    b.object_id = a.object_id
																							INNER JOIN sys.types c ON
																								    c.system_type_id = b.system_type_id
																								AND c.user_type_id   = b.user_type_id
																						WHERE
																							     b.name NOT IN ('BI_HFR','LoadDateTime','ProcessExecutionID')
																							 AND a.object_id = @sourceObjectId
																						ORDER BY
																							b.column_id ASC
																						FOR XML PATH(''), TYPE
																					).value('.', 'VARCHAR(MAX)'), 1, 0, ''
																				) + N'
																			INTO ' + @destinationTempHash + N'
																			FROM ' + @sourceObject + N' a';
									IF(LEN(RTRIM(LTRIM(@dateColumn))) > 0)
										BEGIN
											SET @sqlScript = @sqlScript + 	N' WHERE ';
											SET @sqlScript = @sqlScript +     @dateColumn + N' >= DATEADD(MONTH,-' + @monthsBack + N',GETDATE())';
										END	
								
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 4;
											SET @scriptCode   = N'COD-3600';
											SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Executing SQL Script';
											SET @logType      = N'Information';
											SET @logScript    = @sqlScript;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
													VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@logProcess,10,1);
										END 
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
								
								EXEC(@sqlScript);
								
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 4;
											SET @scriptCode   = N'COD-3700';
											SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Temp Hash Table (' + @destinationTempHash + ') created sucessfully';
											SET @logType      = N'Information';
											SET @logScript    = N'';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
													VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@logProcess,10,1);
										END 
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									SET @logTreeLevel = 4;
									SET @scriptCode   = N'COD-3800';
									SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Error while trying to create Temp Hash Table (' + @destinationTempHash + ')';
									SET @logType      = N'ERROR';
									SET @logScript    = N'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),N'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),N'') + N') - '+ ISNULL(ERROR_MESSAGE(),N'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
											VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@logProcess,11,1);
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							END CATCH
							
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = N'COD-3900';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END Generating Temp Hash Table';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END 
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
				
				--DROPPING DESTINATION TABLE
					IF(@continue = 1)
						BEGIN
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = N'COD-4000';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN dropping Destination table';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END 
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							 BEGIN TRY	
									IF(OBJECT_ID(@destinationObject) IS NOT NULL)
										BEGIN
											--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 4;
														SET @scriptCode   = N'COD-4100';
														SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Destination Table (' + @destinationObject +  ') found';
														SET @logType      = N'Information';
														SET @logScript    = N'';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
																VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@logProcess,10,1);
													END 
											--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
		
											SET @sqlScript = 'DROP TABLE ' + @destinationObject;
											
											--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 4;
														SET @scriptCode   = N'COD-4200';
														SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Executing SQL Script';
														SET @logType      = N'Information';
														SET @logScript    = @sqlScript;
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
																VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@logProcess,10,1);
													END 
											--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
										
											EXEC(@sqlScript);
											
											--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 4;
														SET @scriptCode   = N'COD-4300';
														SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Destination Table (' + @destinationObject + ') dropped successfully';
														SET @logType      = N'Information';
														SET @logScript    = N'';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
																VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@logProcess,10,1);
													END 
											--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
										END
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									SET @logTreeLevel = 4;
									SET @scriptCode   = N'COD-4400';
									SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Error while trying to drop Destination Table (' + @destinationObject + ')';
									SET @logType      = N'ERROR';
									SET @logScript    = N'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),N'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),N'') + N') - '+ ISNULL(ERROR_MESSAGE(),N'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
											VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@logProcess,11,1);
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							END CATCH
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = N'COD-4500';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END dropping Destination table';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END 
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
					
					
				--GENERATING DESTINATION TABLE FROM TEMP TABLE
					IF(@continue = 1)
						BEGIN
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = N'COD-4600';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN generating Destination Table';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END 
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							BEGIN TRY
									SET @sqlScript = 	'SELECT * INTO ' + @destinationObject + ' FROM ' + @destinationTempHash;
									
									--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = N'COD-4700';
												SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Executing SQL Script';
												SET @logType      = N'Information';
												SET @logScript    = @sqlScript;
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
														VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@logProcess,10,1);
											END 
									--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
											
									EXEC(@sqlScript)
									
									--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 4;
												SET @scriptCode   = N'COD-4800';
												SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Destination table (' + @destinationObject + ') generated successfully';
												SET @logType      = N'Information';
												SET @logScript    = N'';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
														VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@logProcess,10,1);
											END 
									--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									SET @logTreeLevel = 4;
									SET @scriptCode   = N'COD-4900';
									SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Error while trying to generate the Destination Table (' + @destinationObject + ')';
									SET @logType      = N'ERROR';
									SET @logScript    = N'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),N'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),N'') + N') - '+ ISNULL(ERROR_MESSAGE(),N'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
											VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@logProcess,11,1);
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							END CATCH
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = N'COD-5000';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END generating Destination Table';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END 
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
				
				--DROPPING TEMP TABLE
					IF(@continue = 1)
						BEGIN
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = N'COD-5100';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'BEGIN dropping Temp Hash Table';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END 
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							BEGIN TRY
									IF(OBJECT_ID(@destinationTempHash) IS NOT NULL)
										BEGIN
											SET @sqlScript = 'DROP TABLE ' + @destinationTempHash;
											
											--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 4;
														SET @scriptCode   = N'COD-5200';
														SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Executing SQL Script';
														SET @logType      = N'Information';
														SET @logScript    = @sqlScript;
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
																VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@logProcess,10,1);
													END 
											--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
									
											EXEC(@sqlScript);
											
											--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 4;
														SET @scriptCode   = N'COD-5300';
														SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Temp Hash Table (' + @destinationTempHash + ') dropped successfully';
														SET @logType      = N'Information';
														SET @logScript    = @sqlScript;
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
																VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@logProcess,10,1);
													END 
											--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
										END
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
									SET @logTreeLevel = 4;
									SET @scriptCode   = N'COD-5400';
									SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'Error while trying to drop Temp Hash Table (' + @destinationObject + ')';
									SET @logType      = N'ERROR';
									SET @logScript    = N'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),N'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),N'') + N') - '+ ISNULL(ERROR_MESSAGE(),N'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
											VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@logProcess,11,1);
								--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
							END CATCH
							--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = N'COD-5500';
										SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END dropping Temp Hash Table';
										SET @logType      = N'Information';
										SET @logScript    = N'';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
												VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@logProcess,10,1);
									END 
							--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						END
					
				IF(@continue = 1)
					BEGIN
						--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 2;
									SET @scriptCode   = N'COD-5600';
									SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'COMMIT TRANSACTION';
									SET @logType      = N'Information';
									SET @logScript    = N'';
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
											VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@logProcess,10,1);
								END
						--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						COMMIT TRANSACTION;
					END
				ELSE
					BEGIN
						--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 2;
									SET @scriptCode   = N'COD-5700';
									SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'ROLLBACK TRANSACTION';
									SET @logType      = N'Information';
									SET @logScript    = N'';
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
											VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@logProcess,10,1);
								END
						--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
						ROLLBACK TRANSACTION;
					END
				--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 1;
							SET @scriptCode   = N'COD-5800';
							SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END Generating Hash Key tables';
							SET @logType      = N'Information';
							SET @logScript    = N'';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
									VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@logProcess,10,1);
						END
				--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>
			END
	--<><><><><><><><><><><><><> BEGIN LOG <><><><><><><><><><><><><>
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 0;
				SET @scriptCode   = N'COD-5900';
				SET @logProcess   = REPLICATE(@logSpaceTree,@logTreeLevel) + N'END Store Procedure';
				SET @logType      = N'Information';
				SET @logScript    = N'';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @generateHashKey_log (executionID,executionDateTime,sourceSchema,sourceObjectName,destinationSchema,destinationObjectName,hashKeyColumns,dateColumn,monthsBack,scriptCode,logType,logProcess,logScript)
						VALUES (@executionID,GETDATE(),@sourceSchema,@sourceObjectName,@destinationSchema,@destinationObjectName,@hashKeyColumns,@dateColumn,@monthsBack,@scriptCode,@logType,@logProcess,@logScript);
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@logProcess,10,1);
			END
	--<><><><><><><><><><><><><> END LOG <><><><><><><><><><><><><><>

	INSERT INTO dbo.generateHashKey_log
		SELECT 
			 executionID
			,sequenceID
			,executionDateTime
			,sourceSchema
			,sourceObjectName
			,destinationSchema
			,destinationObjectName
			,hashKeyColumns
			,dateColumn
			,monthsBack
			,scriptCode
			,logType
			,logProcess
			,logScript
		FROM
			@generateHashKey_log;
	
	--RAISE ERROR IN CASE OF
		IF(@continue = 0)
			BEGIN
				DECLARE @errorMessage NVARCHAR(300);
				
				SET @errorMessage = N'PLEASE CHECK --> SELECT * FROM dbo.generateHashKey_log WHERE executionID = ' + CONVERT(NVARCHAR(20),@executionID);
				
				RAISERROR(@errorMessage,11,1);
			END 
END
