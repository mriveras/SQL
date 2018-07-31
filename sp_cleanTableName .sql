CREATE PROCEDURE dbo.sp_cleanTableName 
AS
BEGIN
	DECLARE
		 @databaseName NVARCHAR(128)
		,@schema       NVARCHAR(128)
		,@charToRemove NVARCHAR(1)
		,@oldTableName NVARCHAR(128)
		,@newTableName NVARCHAR(128)
		,@SQLscript    NVARCHAR(500);
	
	SET @databaseName = (SELECT DB_NAME());
	SET @schema       = 'Files';
	SET @charToRemove = '$';
	
	IF (SELECT CURSOR_STATUS('global','rdftn_cursor')) >= -1
		BEGIN
			DEALLOCATE rdftn_cursor;
		END
	
	DECLARE rdftn_cursor CURSOR LOCAL FOR						
		SELECT 
			 TABLE_NAME
			,CONVERT(NVARCHAR(128),REPLACE(TABLE_NAME,'$',''))
		FROM 
			INFORMATION_SCHEMA.TABLES
		WHERE 
			    TABLE_NAME LIKE '%' + @charToRemove + '%' 
			AND TABLE_SCHEMA  = @schema 
			AND TABLE_CATALOG = @databaseName;
	
	OPEN rdftn_cursor;
	
	FETCH NEXT FROM rdftn_cursor INTO @oldTableName,@newTableName;
	
	WHILE (@@FETCH_STATUS = 0)
		BEGIN
			IF OBJECT_ID (@schema + '.' + @newTableName) IS NOT NULL
				BEGIN
					SET @SQLscript = 'DROP TABLE ' + @schema + '.' + @newTableName;
					EXEC(@SQLscript);
				END 
			
			SET @oldTableName = @schema + '.' + @oldTableName;
			
			EXEC sp_rename @oldTableName, @newTableName;
			
			FETCH NEXT FROM rdftn_cursor INTO @oldTableName,@newTableName;
		END
	
	CLOSE rdftn_cursor;
	
	IF (SELECT CURSOR_STATUS('global','rdftn_cursor')) >= -1
		BEGIN
			DEALLOCATE rdftn_cursor;
		END
END
GO
