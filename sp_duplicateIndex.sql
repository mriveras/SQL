CREATE PROCEDURE dbo.sp_duplicateIndex
	(
		 @fromSchemaName NVARCHAR(128)
		,@fromTableName  NVARCHAR(128)
		,@toSchemaName   NVARCHAR(128)
		,@toTableName    NVARCHAR(128)
	)
AS
BEGIN
	IF(@fromSchemaName IS NULL OR LEN(@fromSchemaName) = 0)
		SET @fromSchemaName = '';
	IF(@fromTableName IS NULL OR LEN(@fromTableName) = 0)
		SET @fromTableName = '';
	IF(@toSchemaName IS NULL OR LEN(@toSchemaName) = 0)
		SET @toSchemaName = '';
	IF(@toTableName IS NULL OR LEN(@toTableName) = 0)
		SET @toTableName = '';
		
	DECLARE
		 @continue  BIT           = 1
		,@message   VARCHAR(1000) = ''
		,@sqlScript NVARCHAR(MAX) = N'';
		
	--CHECKING INPUT VARIABLES
		IF(@fromSchemaName = '')
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error - The parameter @fromSchemaName is mandatory';
			END
		ELSE IF(@fromTableName = '')
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error - The parameter @fromTableName is mandatory';
			END
		ELSE IF(@toSchemaName = '')
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error - The parameter @toSchemaName is mandatory';
			END
		ELSE IF(@toTableName = '')
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error - The parameter @toTableName is mandatory';
			END			
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM 
					sys.objects a INNER JOIN sys.schemas b ON
						    b.name        = @fromSchemaName
						AND a.name        = @fromTableName
						AND b.[schema_id] = a.[schema_id]
				WHERE
					a.type = 'U'
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error - The object (' + @fromSchemaName + '.' + @fromTableName + ') does not exist or is not a valid table';
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM 
					sys.objects a INNER JOIN sys.schemas b ON
						    b.name        = @toSchemaName
						AND a.name        = @toTableName
						AND b.[schema_id] = a.[schema_id]
				WHERE
					a.type = 'U'
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error - The object (' + @toSchemaName + '.' + @toTableName + ') does not exist or is not a valid table';
			END
		ELSE IF(OBJECT_ID(@fromSchemaName + N'.' + @fromTableName) = OBJECT_ID(@toSchemaName + N'.' + @toTableName))
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error - The object From (' + @fromSchemaName + '.' + @fromTableName + ') shult not be the same as To (' + @toSchemaName + '.' + @toTableName + ')';
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM   sys.indexes 
				WHERE  OBJECT_ID = OBJECT_ID(@fromSchemaName + N'.' + @fromTableName)
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The object (' + @fromSchemaName + '.' + @fromTableName + ') has no index';
			END
		
	--GET INDEX SCRIPTS
		IF(@continue = 1)
			BEGIN
				DECLARE @scripts TABLE (			
					 script      NVARCHAR(MAX)
					,indexNumber INT 
				);
				
				INSERT INTO @scripts (script,indexNumber)
					SELECT
						CASE
							WHEN (aaaa.is_primary_key = 1) THEN 'ALTER TABLE ' + aaaa.tableName + ' ADD CONSTRAINT ' + aaaa.indexName + ' PRIMARY KEY NONCLUSTERED (' + aaaa.columns + ')'
							WHEN (aaaa.is_primary_key = 0 AND aaaa.indexType = 1) THEN 'CREATE CLUSTERED INDEX ' + aaaa.indexName + ' ON ' + aaaa.tableName + ' (' + aaaa.columns + ')'
							WHEN (aaaa.is_primary_key = 0 AND aaaa.indexType = 2) THEN 'CREATE INDEX ' + aaaa.indexName + ' ON ' + aaaa.tableName + ' (' + aaaa.columns + ')'
						END AS script
						,aaaa.indexNumber
					FROM
						(
							SELECT
								 aaa.tableName
								,aaa.indexName
								,aaa.indexNumber
								,(
									STUFF(
										(
											SELECT 
												N',[' + bbbx.name + N']'
											FROM
												sys.index_columns aaax INNER JOIN sys.columns bbbx ON
													    bbbx.object_id = aaax.object_id
													AND bbbx.column_id = aaax.column_id
											WHERE 
												    aaax.object_id = aaa.object_id
												AND aaax.index_id  = aaa.index_id
											ORDER BY
												aaax.index_column_id ASC
											FOR XML PATH(''), TYPE
										).value('.', 'VARCHAR(MAX)'), 1, 1, ''
									) 
								) AS columns
								,aaa.indexType
								,aaa.is_primary_key
							FROM
								(
									SELECT
										 @toSchemaName + N'.' + @toTableName AS tableName
										,CASE
											WHEN (aa.is_primary_key = 1) THEN 'PK_'
											WHEN (aa.is_primary_key = 0 AND aa.indexType = 1) THEN 'UX_'
											WHEN (aa.is_primary_key = 0 AND aa.indexType = 2) THEN 'IX_'
										END + dd.name + '_' 
										+ CONVERT(
											 VARCHAR(3)
											,aa.indexNumber
										) AS indexName
										,aa.indexNumber
										,aa.object_id
										,aa.index_id
										,aa.indexType
										,aa.is_primary_key
									FROM
										(
											SELECT
												 a.object_id
												,a.index_id
												,ROW_NUMBER() OVER( 
													ORDER BY 
														a.index_id ASC
												) AS indexNumber
												,a.type AS indexType
												,a.name AS indexNameOld
												,a.is_primary_key
											FROM
												sys.indexes a 
											WHERE
												    a.object_id = OBJECT_ID(@fromSchemaName + N'.' + @fromTableName)
												AND a.name IS NOT NULL
										) aa INNER JOIN sys.objects dd ON
											dd.object_id = aa.object_id
										INNER JOIN sys.schemas ee ON
											ee.[schema_id] = dd.[schema_id]
									) aaa
						) aaaa LEFT JOIN (
							SELECT
								 aa2.indexType
								,aa2.is_primary_key
								,(
									STUFF(
										(
											SELECT 
												N',[' + bbby.name + N']'
											FROM
												sys.index_columns aaay INNER JOIN sys.columns bbby ON
													    bbby.object_id = aaay.object_id
													AND bbby.column_id = aaay.column_id
											WHERE 
												    aaay.object_id = aa2.object_id
												AND aaay.index_id  = aa2.index_id
											ORDER BY
												aaay.index_column_id ASC
											FOR XML PATH(''), TYPE
										).value('.', 'VARCHAR(MAX)'), 1, 1, ''
									) 
								) AS columns
							FROM
								(
									SELECT
										 a2.object_id
										,a2.index_id
										,a2.type AS indexType
										,a2.is_primary_key
									FROM
										sys.indexes a2 
									WHERE
										    a2.object_id = OBJECT_ID(@toSchemaName + N'.' + @toTableName)
										AND a2.name IS NOT NULL
								) aa2
						) bbbb ON
							    bbbb.indexType      = aaaa.indexType
							AND bbbb.is_primary_key = aaaa.is_primary_key
							AND bbbb.columns        = aaaa.columns
					WHERE
						bbbb.columns IS NULL;
			END
		
	--GENERATE THE INDEXES
		IF(
			@continue = 1
			AND EXISTS(
				SELECT 1
				FROM @scripts
			)
		)
			BEGIN
				BEGIN TRANSACTION
				
				BEGIN TRY
					IF (SELECT CURSOR_STATUS('global','DI_cursor')) >= -1
						BEGIN
							DEALLOCATE DI_cursor;
						END
					
					DECLARE DI_cursor CURSOR LOCAL FOR						
						SELECT
							script
						FROM
							@scripts
						ORDER BY
							indexNumber ASC;
					
					OPEN DI_cursor;
					
					FETCH NEXT FROM DI_cursor INTO @sqlScript;
					
					WHILE (@@FETCH_STATUS = 0)
						BEGIN
							EXEC(@sqlScript);
							
							FETCH NEXT FROM DI_cursor INTO @sqlScript;
						END
					
					CLOSE DI_cursor;
					
					IF (SELECT CURSOR_STATUS('global','DI_cursor')) >= -1
						BEGIN
							DEALLOCATE DI_cursor;
						END
					
					COMMIT TRANSACTION
					
					SET @message = 'Indexes created successfully on ' + @toSchemaName + '.' + @toTableName;
				END TRY
				BEGIN CATCH
					ROLLBACK TRANSACTION
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
				END CATCH
			END
		ELSE IF(@continue = 1)
			BEGIN
				SET @continue = 2;
				SET @message  = 'The table (' + @toSchemaName + '.' + @toTableName + ') has the same indexes as (' + @fromSchemaName + '.' + @fromTableName + ')';
			END

	IF(@continue = 0)
		BEGIN
			SELECT @message;
			RAISERROR(@message,11,1);
		END
	ELSE
		BEGIN
			SELECT @message;
			RAISERROR(@message,10,1);
		END
END
GO
