CREATE PROCEDURE dbo.sp_duplicateIndexesConstraints
	(
		 @fromSchemaName    NVARCHAR(128)
		,@fromTableName     NVARCHAR(128)
		,@toSchemaName      NVARCHAR(128)
		,@toTableName       NVARCHAR(128)
		,@columnConstraints BIT           = 0
		,@indexes           BIT           = 0
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
		 @continue   BIT           = 1
		,@message    VARCHAR(500)  = ''
		,@message_I  VARCHAR(1000) = ''
		,@message_C  VARCHAR(1000) = ''
		,@sqlScript  NVARCHAR(MAX) = N''
		,@INT        INT           = 0;
		
	--CHECKING INPUT VARIABLES
		IF(@fromSchemaName = '')
			BEGIN
				SET @continue   = 0;
				SET @message    = 'Error - The parameter @fromSchemaName is mandatory';
			END
		ELSE IF(@fromTableName = '')
			BEGIN
				SET @continue   = 0;
				SET @message    = 'Error - The parameter @fromTableName is mandatory';
			END
		ELSE IF(@toSchemaName = '')
			BEGIN
				SET @continue   = 0;
				SET @message    = 'Error - The parameter @toSchemaName is mandatory';
			END
		ELSE IF(@toTableName = '')
			BEGIN
				SET @continue   = 0;
				SET @message    = 'Error - The parameter @toTableName is mandatory';
			END	
		ELSE IF(@indexes IS NULL)
			BEGIN
				SET @continue   = 0;
				SET @message    = 'Error - The parameter @indexes is mandatory';
			END	
		ELSE IF(@columnConstraints IS NULL)
			BEGIN
				SET @continue   = 0;
				SET @message    = 'Error - The parameter @columnConstraints is mandatory';
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
				SET @continue   = 0;
				SET @message    = 'Error - The object (' + @fromSchemaName + '.' + @fromTableName + ') does not exist or is not a valid table';
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
				SET @continue   = 0;
				SET @message    = 'Error - The object (' + @toSchemaName + '.' + @toTableName + ') does not exist or is not a valid table';
			END
		ELSE IF(OBJECT_ID(@fromSchemaName + N'.' + @fromTableName) = OBJECT_ID(@toSchemaName + N'.' + @toTableName))
			BEGIN
				SET @continue   = 0;
				SET @message    = 'Error - The object From (' + @fromSchemaName + '.' + @fromTableName + ') shult not be the same as To (' + @toSchemaName + '.' + @toTableName + ')';
			END
	
	--VALIDATING THE EXISTENCE OF THE TABLE STRUCTURE
		--INDEXES
			
		
	--COLUMN CONSTRAINTS
		IF(@columnConstraints = 0)
			BEGIN
				SET @message_C = 'Duplicate Column Constraints not activated';
			END
		ELSE IF(@continue = 1 AND @columnConstraints = 1)
			BEGIN
				IF(
					NOT EXISTS(
						SELECT 1
						FROM
							sys.columns a INNER JOIN sys.default_constraints b ON  
							    b.parent_column_id = a.column_id
							AND b.object_id        = a.default_object_id
						WHERE 
							a.object_id = OBJECT_ID(@fromSchemaName + N'.' + @fromTableName)
					)
				)
					BEGIN
						SET @columnConstraints = 0;
						SET @message_C         = 'The object (' + @fromSchemaName + '.' + @fromTableName + ') has no Column Constraint';
					END
				ELSE
					BEGIN
						BEGIN TRANSACTION
						
						BEGIN TRY
							IF (SELECT CURSOR_STATUS('local','DI_cursor')) >= -1
								BEGIN
									DEALLOCATE DI_cursor;
								END
							
							DECLARE DI_cursor CURSOR LOCAL FOR						
								SELECT
									'ALTER TABLE [' + aaaaa.schemaName + '].[' + aaaaa.tableName + '] ADD CONSTRAINT [' + aaaaa.constraintName + '] DEFAULT ' + aaaaa.defaultValue + ' FOR [' + aaaaa.columnName + '];' AS script 
								FROM
									(
										SELECT
											 aaaa.schemaName
											,aaaa.tableName
											,aaaa.defaultValue
											,aaaa.columnName
											,aaaa.schemaName + '_' + aaaa.tableName + '_' + aaaa.columnName + '_' + CONVERT(VARCHAR(5),aaaa.constraintNumber) AS constraintName
										FROM
											(
												SELECT
													 aaa.schemaName
													,aaa.tableName
													,aaa.defaultValue
													,aaa.columnName
													,ROW_NUMBER() OVER( 
														ORDER BY 
															aaa.defaultObjId ASC
													) AS constraintNumber
												FROM
													(
														SELECT
															 @toSchemaName   AS schemaName
															,@toTableName    AS tableName
															,aa.defaultValue
															,aa.columnName
															,aa.defaultObjId
														FROM
															(
																SELECT 
																	 d.name                                       AS constraintName
																	,REPLACE(REPLACE(d.definition,'(',''),')','') AS defaultValue
																	,c.name                                       AS columnName
																	,d.object_id                                  AS defaultObjId
																FROM 
																	sys.objects a INNER JOIN sys.schemas b ON
																		b.[schema_id] = a.[schema_id]
																	INNER JOIN sys.columns c ON 
																		c.object_id = a.object_id
																	INNER JOIN sys.default_constraints d ON 
								 										    d.parent_column_id = c.column_id
																		AND d.object_id        = c.default_object_id
																WHERE 
																	a.OBJECT_ID = OBJECT_ID(@fromSchemaName + N'.' + @fromTableName)
															) aa LEFT JOIN (
																SELECT 
																	 REPLACE(REPLACE(d.definition,'(',''),')','') AS defaultValue
																	,c.name                                       AS columnName
																FROM 
																	sys.objects a INNER JOIN sys.schemas b ON
																		b.[schema_id] = a.[schema_id]
																	INNER JOIN sys.columns c ON 
																		c.object_id = a.object_id
																	INNER JOIN sys.default_constraints d ON 
																		    d.parent_column_id = c.column_id
																		AND d.object_id        = c.default_object_id
																WHERE 
																	a.OBJECT_ID = OBJECT_ID(@toSchemaName + N'.' + @toTableName)
															) bb ON
																    bb.defaultValue = aa.defaultValue
																AND bb.columnName   = aa.columnName
														WHERE 
															bb.columnName IS NULL
													) aaa
												) aaaa
											) aaaaa;
							
							OPEN DI_cursor;
							
							FETCH NEXT FROM DI_cursor INTO @sqlScript;
							
							SET @INT = 0;
							WHILE (@@FETCH_STATUS = 0)
								BEGIN
									SET @INT = @INT + 1;
									EXEC(@sqlScript);
									FETCH NEXT FROM DI_cursor INTO @sqlScript;
								END
							
							CLOSE DI_cursor;
							
							IF (SELECT CURSOR_STATUS('local','DI_cursor')) >= -1
								BEGIN
									DEALLOCATE DI_cursor;
								END
							
							COMMIT TRANSACTION
							
							IF(@INT > 0)
								BEGIN
									SET @message_C = 'A total of ' + CONVERT(VARCHAR(10),@INT) + ' Column Constraint(s) created successfully'
								END
							ELSE
								BEGIN
									SET @message_C = 'The table (' + @toSchemaName + '.' + @toTableName + ') has the same Column Constraints as (' + @fromSchemaName + '.' + @fromTableName + ')';
								END
						END TRY
						BEGIN CATCH
							ROLLBACK TRANSACTION
							SET @continue  = 0;
							SET @message_C = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
			END
		
	--GENERATE THE INDEXES
		IF(@indexes = 0)
			BEGIN
				SET @message_I = 'Duplicate Indexes not activated';
			END
		ELSE IF(@continue = 1 AND @indexes = 1)
			BEGIN
				IF(
					NOT EXISTS(
						SELECT 1
						FROM   sys.indexes 
						WHERE  OBJECT_ID = OBJECT_ID(@fromSchemaName + N'.' + @fromTableName)
					)
				)
					BEGIN
						SET @indexes   = 0;
						SET @message_I = 'The object (' + @fromSchemaName + '.' + @fromTableName + ') has no index';
					END
				ELSE
					BEGIN
						BEGIN TRANSACTION
						
						BEGIN TRY
							IF (SELECT CURSOR_STATUS('local','DI_cursor2')) >= -1
								BEGIN
									DEALLOCATE DI_cursor2;
								END
							
							DECLARE DI_cursor2 CURSOR LOCAL FOR						
								SELECT
									CASE
										WHEN (aaaa.is_primary_key = 1) THEN 'ALTER TABLE ' + aaaa.tableName + ' ADD CONSTRAINT ' + aaaa.indexName + ' PRIMARY KEY NONCLUSTERED (' + aaaa.columns + ')'
										WHEN (aaaa.is_primary_key = 0 AND aaaa.indexType = 1) THEN 'CREATE CLUSTERED INDEX ' + aaaa.indexName + ' ON ' + aaaa.tableName + ' (' + aaaa.columns + ')'
										WHEN (aaaa.is_primary_key = 0 AND aaaa.indexType = 2) THEN 'CREATE INDEX ' + aaaa.indexName + ' ON ' + aaaa.tableName + ' (' + aaaa.columns + ')'
									END AS script
								FROM
									(
										SELECT
											 aaa.tableName
											,CASE
												WHEN (aaa.is_primary_key = 1) THEN 'PK_'
												WHEN (aaa.is_primary_key = 0 AND aaa.indexType = 1) THEN 'UX_'
												WHEN (aaa.is_primary_key = 0 AND aaa.indexType = 2) THEN 'IX_'
											END + aaa.objectName + '_' 
											+ CONVERT(
												 VARCHAR(3)
												,aaa.indexNumber
											) AS indexName
											,aaa.indexNumber
											,aaa.columns
											,aaa.indexType
											,aaa.is_primary_key						
											
										FROM
											(
												SELECT
													 aa.tableName
													,aa.objectName
													,aa.object_id
													,aa.index_id
													,aa.indexType
													,aa.indexNameOld
													,aa.is_primary_key
													,ROW_NUMBER() OVER( 
														ORDER BY 
															aa.index_id ASC
													) AS indexNumber
													,aa.columns
												FROM
													(
														SELECT
															DISTINCT
															 a.object_id
															,a.index_id
															,a.type AS indexType
															,a.name AS indexNameOld
															,a.is_primary_key
															,STUFF(
																(
																	SELECT 
																		N',[' + bbby.name + N']'
																	FROM
																		sys.index_columns aaay INNER JOIN sys.columns bbby ON
																			    bbby.object_id = aaay.object_id
																			AND bbby.column_id = aaay.column_id
																	WHERE 
																		    aaay.object_id = b.object_id
																		AND aaay.index_id  = b.index_id
																	ORDER BY
																		aaay.index_column_id ASC
																	FOR XML PATH(''), TYPE
																).value('.', 'VARCHAR(MAX)'), 1, 1, ''
															) columns
															,@toSchemaName + N'.' + @toTableName AS tableName
															,@toTableName AS objectName
														FROM
															sys.indexes a INNER JOIN sys.index_columns b ON
																    a.object_id = OBJECT_ID(@fromSchemaName + N'.' + @fromTableName)
																AND b.object_id = a.object_id
																AND b.index_id  = a.index_id
														WHERE 
															a.name IS NOT NULL
													) aa LEFT JOIN (
														SELECT
															a.type AS indexType
															,a.is_primary_key
															,STUFF(
																(
																	SELECT 
																		N',[' + bbby.name + N']'
																	FROM
																		sys.index_columns aaay INNER JOIN sys.columns bbby ON
																			    aaay.object_id = a.object_id
																			AND bbby.object_id = aaay.object_id
																			AND bbby.column_id = aaay.column_id
																	ORDER BY
																		aaay.index_column_id ASC
																	FOR XML PATH(''), TYPE
																).value('.', 'VARCHAR(MAX)'), 1, 1, ''
															) columns
														FROM
															sys.indexes a 
														WHERE 
															    a.object_id = OBJECT_ID(@toSchemaName + N'.' + @toTableName)
															AND	a.name IS NOT NULL
													) bb ON
														    bb.columns        = aa.columns
														AND bb.is_primary_key = aa.is_primary_key
														AND bb.indexType      = aa.indexType
												WHERE
													bb.columns IS NULL
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
									bbbb.columns IS NULL
								ORDER BY
									aaaa.indexNumber ASC;
							
							OPEN DI_cursor2;
							
							FETCH NEXT FROM DI_cursor2 INTO @sqlScript;
							
							SET @INT = 0;
							WHILE (@@FETCH_STATUS = 0)
								BEGIN
									SET @INT = @INT + 1;
									EXEC(@sqlScript);
									FETCH NEXT FROM DI_cursor2 INTO @sqlScript;
								END
							
							CLOSE DI_cursor2;
							
							IF (SELECT CURSOR_STATUS('local','DI_cursor2')) >= -1
								BEGIN
									DEALLOCATE DI_cursor2;
								END
							
							COMMIT TRANSACTION
							
							IF(@INT > 0)
								BEGIN
									SET @message_I = 'A total of ' + CONVERT(VARCHAR(10),@INT) + ' Index(es) created successfully'
								END
							ELSE
								BEGIN
									SET @message_I = 'The table (' + @toSchemaName + '.' + @toTableName + ') has the same Indexes as (' + @fromSchemaName + '.' + @fromTableName + ')';
								END
						END TRY
						BEGIN CATCH
							ROLLBACK TRANSACTION
							SET @continue  = 0;
							SET @message_I = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
			END
	
	IF(@continue = 0)
		BEGIN
			IF(CHARINDEX(@message,'Error') > 0)
				BEGIN
					SET @message = 'ERROR';
					RAISERROR(@message,11,1);
				END
			ELSE IF(CHARINDEX(@message_C,'Error') > 0)
				BEGIN
					SET @message = 'ERROR - Column Constraint';
					RAISERROR(@message_C,11,1);
				END
			ELSE IF(CHARINDEX(@message_I,'Error') > 0)
				BEGIN
					SET @message = 'ERROR - Indexes';
					RAISERROR(@message_I,11,1);
				END
		END
	ELSE
		BEGIN
			SET @message = 'Success';
			RAISERROR('Success',10,1);
		END
	
	SELECT @message AS [message],@message_C AS ColumnConstraint, @message_I AS Indexes;
END
GO
