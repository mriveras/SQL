CREATE PROCEDURE dbo.sp_removeColumns 
	(
		 @schema           NVARCHAR(128)
		,@table            NVARCHAR(128)
		,@columnsToExclude NVARCHAR(1000)
	)
AS
BEGIN
	DECLARE
		 @continue      SMALLINT
		,@message       NVARCHAR(300)
		,@sqlScripts    NVARCHAR(MAX)
		,@fullTableName NVARCHAR(246)
		,@tempTable     NVARCHAR(246);
		
	SET @continue = 1;
	
	--CHECKING INPUT PARAMETERS
		IF(@schema IS NULL OR LEN(RTRIM(LTRIM(@schema))) = 0)
			BEGIN
				SET @continue = 0;
				SET @message  = N'The @schema input parameter is required'
				RAISERROR(@message,11,1);
			END
		ELSE IF(SCHEMA_ID(@schema) IS NULL)
			BEGIN
				SET @continue = 0;
				SET @message  = N'The schema ' + @schema + ' does not exists'
				RAISERROR(@message,11,1);
			END
		ELSE IF(@table IS NULL OR LEN(RTRIM(LTRIM(@table))) = 0)
			BEGIN
				SET @continue = 0;
				SET @message  = N'The @table input parameter is required'
				RAISERROR(@message,11,1);
			END
		ELSE IF(@columnsToExclude IS NULL OR RTRIM(LTRIM(LEN(@columnsToExclude))) = 0)
			BEGIN
				SET @continue = 0;
				SET @message  = N'The @columnsToExclude input parameter is required'
				RAISERROR(@message,11,1);
			END
			
		IF(
			@continue = 1
			AND NOT EXISTS(
				SELECT 1
				FROM sys.objects 
				WHERE 
					    type      = 'U'
					AND object_id = OBJECT_ID(@schema + N'.' + @table)
			)
		)
		 BEGIN
		 	SET @continue = 0;
 			SET @message  = N'The object ' + @schema + '.' + @table + ' does not exists or is not a Table'
			RAISERROR(@message,11,1);
		 END	
	
	IF(@continue = 1)
		BEGIN
			SET @fullTableName = @schema + N'.' + @table;
			SET @tempTable     = @schema + N'.' + @table + + N'_TMPPRC';
			
			BEGIN TRANSACTION
			
			BEGIN TRY
				IF(OBJECT_ID(@tempTable) IS NOT NULL)
					BEGIN
						SET @sqlScripts = N'DROP TABLE ' + @tempTable;
						EXEC(@sqlScripts);
					END
				
				SELECT
					@sqlScripts = N'SELECT ' + 
					CONVERT(NVARCHAR(max),
						STUFF(
							(
								SELECT
									', ' + name
								FROM
									sys.columns a
								WHERE 
									a.object_id = OBJECT_ID(@fullTableName)
									AND a.name NOT IN (SELECT Item FROM dbo.udf_DelimitedSplit8K(@columnsToExclude,','))
								ORDER BY 
									a.column_id asc
							FOR XML PATH(''), TYPE
							).value('.', 'VARCHAR(MAX)'), 1, 2, ''
						)
					) +
					N' INTO ' + @tempTable +
					N' FROM ' + @fullTableName;
				
				EXEC(@sqlScripts);
				
				IF(OBJECT_ID(@tempTable) IS NOT NULL)
					BEGIN
						SET @sqlScripts = N'DROP TABLE ' + @fullTableName;
						EXEC(@sqlScripts);
					END
				
				SET @sqlScripts = N'SELECT * INTO ' + @fullTableName + ' FROM ' + @tempTable;
				EXEC(@sqlScripts);
				
				IF(OBJECT_ID(@tempTable) IS NOT NULL)
					BEGIN
						SET @sqlScripts = N'DROP TABLE ' + @tempTable;
						EXEC(@sqlScripts);
					END
				
				COMMIT TRANSACTION;
			END TRY
			BEGIN CATCH
				SET @continue = 0;
				SET @message  = N'SQL Error: ' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),N'') + N' - '+ ISNULL(ERROR_MESSAGE(),N'');
				RAISERROR(@message,11,1);
				ROLLBACK TRANSACTION;
			END CATCH
		END
END
GO
