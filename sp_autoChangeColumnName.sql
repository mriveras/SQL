CREATE PROCEDURE dbo.sp_autoChangeColumnName 
	(
		 @fullSchemaName      NVARCHAR(128) = ''
		,@objectSchema        NVARCHAR(128) = ''
		,@objectName          NVARCHAR(128) = ''
		,@charactersToFind    NVARCHAR(128) = ''
		,@charactersToReplace NVARCHAR(128) = ''
	)
AS
BEGIN
	DECLARE @tablesToProcess TABLE (
		 schemaName    NVARCHAR(128)
		,objectName    NVARCHAR(128)
		,columnName    NVARCHAR(128)
		,columnNameNew NVARCHAR(128)
	);
	
	DECLARE
		 @continue            SMALLINT      = 1
		,@message             NVARCHAR(300) = ''
		,@objectId            INT           = 0
		,@sqlScripts          NVARCHAR(MAX) = N''
		,@fullSchemaExecution BIT           = 1
		,@schemaName_C        NVARCHAR(128) = N''
		,@objectName_C        NVARCHAR(128) = N''
		,@columnName_C        NVARCHAR(128) = N''
		,@columnNameNew_C     NVARCHAR(128) = N'';
	
	IF(CHARINDEX('CHAR(',@charactersToFind) > 0)
		BEGIN
			BEGIN TRY
				DECLARE @charactersToFind2 NVARCHAR(128);
				
				SET @sqlScripts = N'SELECT @charResult = ' + @charactersToFind;
				EXECUTE sp_executesql @sqlScripts,N'@charResult NVARCHAR(128) OUTPUT',@charResult = @charactersToFind2 OUTPUT;
				
				SET @charactersToFind = @charactersToFind2;
			END TRY
			BEGIN CATCH
				--Nothing Happens, No CHAR assignation found
			END CATCH
		END
	
	--CHECKING INPUT PARAMETERS
		IF(@continue = 1 AND LEN(@fullSchemaName) = 0 AND (LEN(@objectSchema) = 0 OR LEN(@objectName) = 0))
			BEGIN
				SET @continue = 0;
				SET @message = 'Parameters required @fullSchemaName or (@objectSchema and @objectName)'
				RAISERROR(@message,11,1);
			END
		
		IF(DATALENGTH(@charactersToFind) = 0)
			BEGIN
				SET @continue = 0;
				SET @message = 'The input parameter @charactersToFind can not be empty'
				RAISERROR(@message,11,1);
			END
			
		IF(@continue = 1 AND LEN(@objectSchema) > 0 AND LEN(@objectName) > 0)
			BEGIN
				IF(SCHEMA_ID(@objectSchema) IS NULL)
					BEGIN
						SET @continue = 0;
						SET @message = 'The input parameter @objectSchema (' + @objectSchema + ') has an invalid schema'
						RAISERROR(@message,11,1);
					END
				ELSE
					BEGIN 
						SET @objectId = OBJECT_ID(@objectSchema + '.' + @objectName)
						IF(@objectId IS NULL)
							BEGIN
								SET @continue = 0;
								SET @message = 'The object (' + @objectSchema + '.' + @objectName + ') does not exist'
								RAISERROR(@message,11,1);
							END 
						ELSE
							BEGIN
								SET @fullSchemaExecution = 0;
							
							--INSERTING COLUMNS DETAILS TO PROCESS
								INSERT INTO @tablesToProcess (schemaName,objectName,columnName,columnNameNew)
									SELECT 
										 DISTINCT
										 b.name                                                 AS schemaName
										,a.name                                                 AS objectName
										,c.name                                                 AS columnName
										,REPLACE(c.name,@charactersToFind,@charactersToReplace) AS columnNameNew
									FROM    sys.objects a INNER JOIN sys.schemas b ON
												b.[schema_id] = a.[schema_id]
											INNER JOIN sys.columns c ON
												c.object_id = a.object_id 
									WHERE
											a.object_id = @objectId
										AND c.name LIKE N'%' + @charactersToFind + N'%';
							END
					END 
			END

		IF(@continue = 1 AND @fullSchemaExecution = 1 AND LEN(@fullSchemaName) > 0)
			BEGIN
				IF(SCHEMA_ID(@fullSchemaName) IS NULL)
					BEGIN
						SET @continue = 0;
						SET @message = 'The Schema (' + @fullSchemaName + ') does not exist'
						RAISERROR(@message,11,1);
					END
				ELSE
					BEGIN
					--INSERTING COLUMNS DETAILS TO PROCESS
						INSERT INTO @tablesToProcess (schemaName,objectName,columnName,columnNameNew)
							SELECT 
								 DISTINCT
								 b.name                                                 AS schemaName
								,a.name                                                 AS objectName
								,c.name                                                 AS columnName
								,REPLACE(c.name,@charactersToFind,@charactersToReplace) AS columnNameNew
							FROM    sys.objects a INNER JOIN sys.schemas b ON
										b.[schema_id] = a.[schema_id]
									INNER JOIN sys.columns c ON
										c.object_id = a.object_id 
							WHERE
								    b.name = @fullSchemaName
								AND c.name LIKE N'%' + @charactersToFind + N'%';
					END
			END
	
	--RENAMING COLUMNS
		IF(@continue = 1)
			BEGIN				
				IF (SELECT CURSOR_STATUS('LOCAL','ACCN_CURSOR')) >= -1
					DEALLOCATE ACCN_CURSOR;
			
				DECLARE ACCN_CURSOR CURSOR LOCAL FOR 
					SELECT
						 schemaName
						,objectName
						,columnName
						,columnNameNew
					FROM 
						@tablesToProcess;
				
				OPEN ACCN_CURSOR;
				
				FETCH NEXT FROM ACCN_CURSOR INTO @schemaName_C, @objectName_C, @columnName_C, @columnNameNew_C;
			
				WHILE (@continue = 1 AND @@FETCH_STATUS = 0)
					BEGIN
						SET @sqlScripts = N'EXEC sp_rename ''' + @schemaName_C + '.' + @objectName_C + '.' + @columnName_C + ''',''' + @columnNameNew_C + ''',''COLUMN''';
						
						BEGIN TRY
							EXEC(@sqlScripts);
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
							RAISERROR(@message,11,1);
						END CATCH
							
						FETCH NEXT FROM ACCN_CURSOR INTO @schemaName_C, @objectName_C, @columnName_C, @columnNameNew_C;
					END
				
				CLOSE ACCN_CURSOR;
				
				IF (SELECT CURSOR_STATUS('LOCAL','ACCN_CURSOR')) >= -1
					DEALLOCATE ACCN_CURSOR;
			END
END
GO
