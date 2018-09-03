ALTER PROCEDURE dbo.sp_reCreateSequenceColumn 
	(
		 @tableName             NVARCHAR(256) = N''
		,@sequenceColumnName    NVARCHAR(128) = N''
		,@keyColumnNameOptional NVARCHAR(128) = N''
		,@keyValueOptional      NVARCHAR(100) = N''
		,@orderByColumnName     NVARCHAR(128) = N''
		,@status                BIT                 OUTPUT
		,@message               VARCHAR(1000)       OUTPUT
		,@SQL                   VARCHAR(4000)       OUTPUT
		
	)
AS
BEGIN
	DECLARE
	--PROCESS FLOW VARIABLES
		 @continue                 BIT            = 1
		,@sqlScript                NVARCHAR(MAX)  = N''
	--FLAG VARIABLES
		,@hasKey                   BIT            = 0
		,@noColumns                BIT            = 0
		,@sequenceColumnIsIdentity BIT            = 0
	--GENERAL VARIABLES
		,@BIT                      BIT            = 0
		,@tempTableName            NVARCHAR(256)  = @tableName + '_RCSC_TEMP'
		,@columns                  NVARCHAR(4000) = N''
		,@sequenceColumnDatType    NVARCHAR(128)  = N'';
	
	SET @status  = 0;
	SET @message = '';
	SET @SQL     = '';
	
	IF(LEN(@keyColumnNameOptional) > 0)
		BEGIN
			SET @hasKey = 1;
		END
	
	--CHECKING INPUT VARIABLES
		IF(
			NOT EXISTS(
				SELECT  1
				FROM    sys.objects a
				WHERE 
					    a.object_id = OBJECT_ID(@tableName)
					AND a.type      = 'U'
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Table (' + @tableName + ') does not exist';
			END
		ELSE IF(
			NOT EXISTS(
				SELECT  1
				FROM    sys.columns a
				WHERE 
					    a.object_id = OBJECT_ID(@tableName)
					AND a.name      = @sequenceColumnName
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Sequence Column (' + @sequenceColumnName + ') does not exist in the Table (' + @tableName + ')';
			END
		ELSE IF(
			NOT EXISTS(
				SELECT  1
				FROM    
					sys.columns a INNER JOIN sys.types b ON
						    b.system_type_id = a.system_type_id
						AND b.user_type_id   = a.user_type_id
				WHERE 
					    a.object_id  = OBJECT_ID(@tableName)
					AND a.name       = @sequenceColumnName
					AND b.name      IN ('BIGINT','INT','SMALLINT','TINYINT','NUMERIC')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Sequence Column (' + @sequenceColumnName + ') needs to be BIGINT, INT, SMALLINT, TINYINT or NUMERIC';
			END
		ELSE IF(
			    @hasKey = 1
			AND NOT EXISTS(
				SELECT  1
				FROM    sys.columns a
				WHERE 
					    a.object_id = OBJECT_ID(@tableName)
					AND a.name      = @keyColumnNameOptional
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Key Column (' + @keyColumnNameOptional + ') does not exist in the table (' + @tableName + ')';
			END
		ELSE IF(
			    @hasKey = 1
			AND (
				   @keyValueOptional      IS NULL
				OR LEN(@keyValueOptional) = 0
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Key Value (' + ISNULL(@keyValueOptional,'NULL') + ') is requiered when the Key Column is specified';
			END
		ELSE IF(
			NOT EXISTS(
				SELECT  1
				FROM    sys.columns a
				WHERE 
					    a.object_id = OBJECT_ID(@tableName)
					AND a.name      = @orderByColumnName
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Order By Column (' + @orderByColumnName + ') does not exist in the table (' + @tableName + ')';
			END
		
	--VALIDATING THAT THE KEY VALUES HAS RECORDS IN THE TABLE
		IF(
			    @continue = 0 
			AND @hasKey   = 1
		)
			BEGIN
				IF(
					NOT EXISTS(
						SELECT 1
						FROM 
							sys.columns a INNER JOIN sys.types b ON
								    b.system_type_id = a.system_type_id
								AND b.user_type_id   = a.user_type_id
						WHERE
							    a.object_id    = OBJECT_ID(@tableName)
							AND a.name         = @keyColumnNameOptional
							AND UPPER(b.name) IN ('BIGINT','BIT','DECIMAL','FLOAT','INT','MONEY','NUMERIC','REAL','SMALLINT','SMALLMONEY','TINYINT')
					)
				)
					BEGIN
						SET @keyValueOptional = '''' + @keyValueOptional + ''''
					END
				
				BEGIN TRY
					SET @sqlScript = 'SELECT @intBIT = 1 FROM ' + @tableName + ' WHERE ' + @keyColumnNameOptional + ' = ' + @keyValueOptional;
					EXEC sp_executesql @sqlScript, N'@intBIT BIT OUTPUT', @intBIT = @BIT OUTPUT;
				
					IF(@BIT = 0)
						BEGIN
							SET @continue = 0;
							SET @message  = 'The value (' + @keyValueOptional + ') does not have records in over the Column (' + @keyColumnNameOptional + ') in the Table (' + @tableName + ')';
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = @sqlScript
				END CATCH
			END
	
	--GETTING THE COLUMNS OF THE TABLE
		IF(@continue = 1)
			BEGIN
				BEGIN TRY
					SET @sqlScript = 'SELECT
						@intColumns = STUFF(
							(
								SELECT   
									N'','' + a.name
								FROM     
									sys.columns a 
								WHERE
									    a.object_id = OBJECT_ID(''' + @tableName + ''')
									AND a.name NOT IN (''' + @sequenceColumnName + ''')
								ORDER BY 
									a.name ASC
								FOR XML  PATH(''''), TYPE
							).value(''.'', ''VARCHAR(MAX)''), 1, 1, ''''
						)';
						
					EXEC sp_executesql @sqlScript, N'@intColumns NVARCHAR(4000) OUTPUT', @intColumns = @columns OUTPUT;
					
					IF(LEN(@columns) = 0)
						BEGIN
							SET @continue = 0;
							SET @message  = 'The Table (' + @tableName + ') has no Columns apart of the Sequence Column (' + @sequenceColumnName + ')';
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = @sqlScript
				END CATCH
			END
	
	--GETTING SEQUENCE COLUMN DATA TYPE
		IF(@continue = 1)
			BEGIN
				BEGIN TRY 
					SET @sequenceColumnDatType = (
						SELECT
							CASE
								WHEN b.[precision] = 0 THEN --Data Types Strings and TimeStamp
									CASE
										WHEN (UPPER(b.name) IN ('TIMESTAMP','UNIQUEIDENTIFIER')) THEN
											UPPER(b.name) --TimeStamp / Uniqueidentifier Columns	
										WHEN (UPPER(b.name) IN ('NVARCHAR','NCHAR','NTEXT')) THEN
											CASE
												WHEN (a.max_length = -1) THEN
													UPPER(b.name) +  '(MAX)' --Nvarchar, Nchar and Ntext Columns with MAX length
												ELSE
													UPPER(b.name) +  '(' + CONVERT(NVARCHAR(10),a.max_length / 2) + ')' --Nvarchar, Nchar and Ntext Columns 
												END
										ELSE 
											CASE
												WHEN(a.max_length = -1) THEN
													UPPER(b.name) +  '(MAX)' --String Columns with MAX length
												ELSE
													UPPER(b.name) +  '(' + CONVERT(NVARCHAR(10),a.max_length) + ')' --String Columns 
											END
									END
								ELSE --Data Types Non-Strings (such as Decimal,INT,FLOAT,...)
									CASE
										WHEN (UPPER(b.name) IN ('NUMERIC','DECIMAL')) THEN 
											UPPER(b.name) +  '(' + CONVERT(NVARCHAR(10),a.precision) + ',' + CONVERT(NVARCHAR(10),a.scale) + ')' --Non-Strings Columns
										ELSE
											UPPER(b.name) --Non-Strings Columns
									END						
							END
						FROM
							sys.columns a INNER JOIN sys.types b ON
								    b.system_type_id = a.system_type_id
								AND b.user_type_id   = a.user_type_id
						WHERE
							    a.object_id = OBJECT_ID(@tableName)
							AND a.name = @sequenceColumnName
					);
					IF(LEN(@sequenceColumnDatType) = 0)
						BEGIN
							SET @continue = 0;
							SET @message  = 'Error trying to get the data type of the Sequence Column (' + @sequenceColumnName + ')';
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = @sqlScript
				END CATCH
			END
	
	--CHECK IF SEQUENCE COLUMN IS IDENTITY.
		IF(@continue = 1)
			BEGIN
				IF(
					EXISTS(
						SELECT 1
						FROM sys.identity_columns a
						WHERE 
							    a.object_id = OBJECT_ID(@tableName)
							AND a.is_identity = 1
					)
				)
					BEGIN
						SET @sequenceColumnIsIdentity = 1;
					END
			END 
	
	--IF THE SEQUENCE COLUMN IS IDENTITY AND THE PROCESS HAS A KEY TO FILTER RESULTS, THROW AN ERROR BECAUSE THE IDENTITY COLUMN HAS TO BE ENTIRELY RECREATED SO THE KEY IS NOT ALLOWED.
		IF(@sequenceColumnIsIdentity = 1 AND @hasKey = 1)
			BEGIN
				SET @continue = 0;
				SET @message  = 'Error the Sequence Column (' +  + ') is IDENTITY, for that reason the Key filtering featured are not allowed. Please do not specify @keyColumnNameOptional and @keyValueOptional.';
			END
		
	--DROP TEMP PHYSICAL TABLE
		IF(
			    @continue = 1
			AND OBJECT_ID(@tempTableName) IS NOT NULL
		)
			BEGIN
				BEGIN TRY
					SET @sqlScript = 'DROP TABLE ' + @tempTableName;
					EXEC(@sqlScript);
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = @sqlScript
				END CATCH
			END
	
	--CREATE TEMP PHYSICAL TABLE RECREATING SEQUENCE COLUMN
		IF(@continue = 1)
			BEGIN
				BEGIN TRY
					SET @sqlScript = 'SELECT ' + @columns + ' INTO ' + @tempTableName + ' FROM ' + @tableName + ' WHERE 1 = 0';
					EXEC(@sqlScript)
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = @sqlScript
				END CATCH
			END
	
	--CREATING SECUENCE COLUMN IN THE TEMP TABLE
		IF(@continue = 1)
			BEGIN
				BEGIN TRY
					SET @sqlScript = 'ALTER TABLE ' + @tempTableName + ' ADD ' + @sequenceColumnName + ' ' + @sequenceColumnDatType;
					EXEC(@sqlScript)
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = @sqlScript
				END CATCH
			END
	
	--RECREATING SEQUENCE COLUMN
		IF(@continue = 1)
			BEGIN
				BEGIN TRY				
					SET @sqlScript = 'INSERT INTO ' + @tempTableName + ' (' + @columns + ',' + @sequenceColumnName + ')
						SELECT
							' + @columns + '
							,ROW_NUMBER() OVER(
								ORDER BY ' + @orderByColumnName + ' ASC
							) -1 AS ' + @sequenceColumnName + '
						FROM
							' + @tableName
						
					IF(@hasKey = 1)
						BEGIN
							SET @sqlScript = @sqlScript + ' WHERE ' + @keyColumnNameOptional + ' = ' + @keyValueOptional + ' ';
						END
						
					SET @sqlScript = @sqlScript + ' ORDER BY
							' + @orderByColumnName + ' ASC';
					EXEC(@sqlScript);
					SELECT @sqlScript
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = @sqlScript
				END CATCH
			END

	IF(@continue = 1)
		BEGIN
			BEGIN TRANSACTION
			BEGIN TRY
				--DELETING DATA 
					IF(@hasKey = 1)
						BEGIN
							SET @sqlScript = 'DELETE FROM ' + @tableName + ' WHERE ' + @keyColumnNameOptional + ' = ' + @keyValueOptional;
						END
					ELSE
						BEGIN
							SET @sqlScript = 'TRUNCATE TABLE ' + @tableName;
						END
					EXEC(@sqlScript)
				
				--IF THE SEQUENCE COLUMN IS AN IDENTITY, RESEED THE SEQUENCE COLUMN
					IF(@sequenceColumnIsIdentity = 1)
						BEGIN
							SET @sqlScript = 'DBCC CHECKIDENT (''' + @tableName + ''', RESEED, 0)';
							EXEC(@sqlScript)
						END
				
				--INSERTING RECREATING DATA
				SELECT @sequenceColumnIsIdentity;
					IF(@sequenceColumnIsIdentity = 1)
						BEGIN
							SET @sqlScript = 'INSERT INTO ' + @tableName + ' (' + @columns + ') SELECT ' + @columns + ' FROM ' + @tempTableName + ' ORDER BY ' + @sequenceColumnName + ' ASC';
						END
					ELSE
						BEGIN
							SET @sqlScript = 'INSERT INTO ' + @tableName + ' (' + @columns + ',' + @sequenceColumnName + ') SELECT ' + @columns + ',' + @sequenceColumnName + ' FROM ' + @tempTableName + ' ORDER BY ' + @sequenceColumnName + ' ASC';
						END
					EXEC(@sqlScript)
			END TRY
			BEGIN CATCH
				SET @continue = 0;
				SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
				SET @SQL      = @sqlScript
			END CATCH
		END	  
	
	--DROP TEMP PHYSICAL TABLE
		IF(OBJECT_ID(@tempTableName) IS NOT NULL)
			BEGIN
				BEGIN TRY
					SET @sqlScript = 'DROP TABLE ' + @tempTableName;
					EXEC(@sqlScript);
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = @sqlScript
				END CATCH
			END			
END
GO

