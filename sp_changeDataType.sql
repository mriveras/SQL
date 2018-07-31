CREATE PROCEDURE dbo.sp_changeDataType
	(
		 @schema      NVARCHAR(128)
		,@table       NVARCHAR(128)
		,@column      NVARCHAR(128)
		,@toDataType  NVARCHAR(128)
		,@toMaxLenght SMALLINT
		,@toPrecision SMALLINT
	)
AS
BEGIN
	IF(@schema IS NULL)
		SET @schema = '';
	IF(@table IS NULL)
		SET @table = '';
	IF(@column IS NULL)
		SET @column = '';
	IF(@toDataType IS NULL)
		SET @toDataType = '';
	IF(@toMaxLenght IS NULL)
		SET @toMaxLenght = 0;
	IF(@toPrecision IS NULL)
		SET @toPrecision = 0;
		
	DECLARE
		 @continue   BIT           = 1
		,@message    VARCHAR(1000) = ''
		,@execScript NVARCHAR(MAX) = N'';
	
	DECLARE @scripts TABLE (
		 executionOrder INT
		,script         NVARCHAR(MAX)
	);
	
	--VALIDATING INPUT PARAMETERS
		IF(SCHEMA_ID(@schema) IS NULL)
			BEGIN
				SET @message  = 'ERROR - Input Parameter @schema (' + @schema + ') is not valid'
				SET @continue = 0;
			END
		ELSE IF(OBJECT_ID(@schema + N'.' + @table) IS NULL)
			BEGIN
				SET @message  = 'ERROR - The specified Table (' + @schema + '.' + @table + ') is not valid'
				SET @continue = 0;
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM sys.columns a
				WHERE 
					    a.object_id = OBJECT_ID(@schema + N'.' + @table)
					AND a.name      = @column
			)
		)
			BEGIN
				SET @message  = 'ERROR - Input Parameter @column (' + @column + ') does not exist in the table (' + @schema + '.' + @table + ')'
				SET @continue = 0;
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM sys.types a
				WHERE  
					    a.[schema_id] = SCHEMA_ID(@schema)
					AND UPPER(a.name) = UPPER(@toDataType)
			)
		)
			BEGIN
				SET @message  = 'ERROR - Input Parameter @toDataType (' + @toDataType + ') is not valid'
				SET @continue = 0;
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM sys.types a
				WHERE
					    a.[schema_id] = SCHEMA_ID(@schema)
				   	AND UPPER(a.name) = UPPER(@toDataType)
					AND (
						(
							UPPER(a.name) IN ('BIT','BINARY','UNIQUEIDENTIFIER','TINYINT','SMALLINT','INT','BIGINT','DATE','TIME','SMALLDATETIME','DATETIME','DATETIME2','DATETIMEOFFSET','MONEY','SMALLMONEY','TIMESTAMP','XML','TEXT','NTEXT','IMAGE','SQL_VARIANT','HIERARCHYID','GEOMETRY','GEOGRAPHY','VARBINARY','SYSNAME')
						)
						OR (
							    UPPER(a.name) IN ('VARCHAR','NVARCHAR','CHAR','NCHAR')
							AND a.max_length  >= @toMaxLenght
						)
						OR (
							    UPPER(a.name) IN ('REAL','FLOAT','DECIMAL','NUMERIC')
							AND a.max_length  >= @toMaxLenght
							AND a.[precision] >= @toPrecision
						)
					)					
			)
		)
			BEGIN
				SET @message  = 'ERROR - Input Parameters @toDataType (' + @toDataType + '), @toMaxLenght (' + CONVERT(VARCHAR(10),@toMaxLenght) + ') and @toPrecision (' + CONVERT(VARCHAR(10),@toPrecision) + ') are not valid'
				SET @continue = 0;
			END
	
	IF(@continue = 1)
		BEGIN
		--COLUMN CONSTRAINTS
			IF (SELECT CURSOR_STATUS('LOCAL','CDT_CURSOR')) >= -1
				DEALLOCATE CDT_CURSOR;
			
			DECLARE CDT_CURSOR CURSOR FOR 
				WITH constraintsList(
					 schemaName
					,tableName
					,constraintName
					,defaultValue
					,columnName
				) AS (
					SELECT 
						 b.name       AS schemaName
						,a.name       AS tableName
						,d.name       AS constraintName
						,d.definition AS defaultValue
						,c.name       AS columnName
					FROM 
						sys.objects a INNER JOIN sys.schemas b ON
							b.[schema_id] = a.[schema_id]
						INNER JOIN sys.columns c ON
							c.object_id = a.object_id 
						INNER JOIN sys.default_constraints d ON
							    d.parent_column_id = c.column_id
							AND d.object_id        = c.default_object_id
					WHERE 
						    a.object_id = OBJECT_ID(@schema + N'.' + @table)
						AND c.name      = @column
				) 
				SELECT
					xa.script
				FROM
					(
							SELECT
								 1 AS executionOrder
								,'ALTER TABLE [' + a.schemaName + '].[' + a.tableName + '] DROP CONSTRAINT [' + a.constraintName + '];' AS script
							FROM 
								constraintsList a
						UNION ALL
							SELECT
								 2 AS executionOrder
								,CASE
									WHEN (UPPER(@toDataType) IN ('BIT','BINARY','UNIQUEIDENTIFIER','TINYINT','SMALLINT','INT','BIGINT','DATE','TIME','SMALLDATETIME','DATETIME','DATETIME2','DATETIMEOFFSET','MONEY','SMALLMONEY','TIMESTAMP','XML','TEXT','NTEXT','IMAGE','SQL_VARIANT','HIERARCHYID','GEOMETRY','GEOGRAPHY','VARBINARY','SYSNAME')) THEN
										'ALTER TABLE [' + @schema + '].[' + @table + '] ALTER COLUMN [' + @column + '] ' + UPPER(@toDataType) + ';'
									WHEN (UPPER(@toDataType) IN ('VARCHAR','NVARCHAR','CHAR','NCHAR')) THEN
										'ALTER TABLE [' + @schema + '].[' + @table + '] ALTER COLUMN [' + @column + '] ' + UPPER(@toDataType) + '(' + CONVERT(VARCHAR(10),@toMaxLenght) + ');'
									WHEN (UPPER(@toDataType) IN ('REAL','FLOAT','DECIMAL','NUMERIC')) THEN
										'ALTER TABLE [' + @schema + '].[' + @table + '] ALTER COLUMN [' + @column + '] ' + UPPER(@toDataType) + '(' + CONVERT(VARCHAR(10),@toMaxLenght) + ',' + CONVERT(VARCHAR(10),@toPrecision) + ');'
								END AS script
						UNION ALL
							SELECT
								 3 AS executionOrder
								,'ALTER TABLE [' + a.schemaName + '].[' + a.tableName + '] ADD CONSTRAINT [' + a.constraintName + '] DEFAULT ' + a.defaultValue + ' FOR [' + a.columnName + '];' AS script
							FROM 
								constraintsList a
					) xa
				ORDER BY
					xa.executionOrder;
			
			OPEN CDT_CURSOR;
			
			FETCH NEXT FROM CDT_CURSOR INTO @execScript;
			
			BEGIN TRANSACTION
				
			BEGIN TRY
				WHILE (@@FETCH_STATUS = 0)
					BEGIN
						SELECT @execScript
						--EXEC(@execScript);
						FETCH NEXT FROM CDT_CURSOR INTO @execScript;
					END
					COMMIT TRANSACTION
			END TRY
			BEGIN CATCH
				SET @message = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
				ROLLBACK TRANSACTION
			END CATCH
		END
END
GO
