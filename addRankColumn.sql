CREATE PROCEDURE dbo.sp_addRankColumn
	(
		 @schema  NVARCHAR(128)
		,@table   NVARCHAR(128)
		,@columns NVARCHAR(1000) = ''
		,@status  TINYINT             OUTPUT
		,@message VARCHAR(2000)       OUTPUT
		,@SQL     VARCHAR(1000)       OUTPUT
	)
AS
/*
	Developed by: Mauricio Rivera
	Date: 16 May 2018
*/
BEGIN
	IF(@schema IS NULL)
		SET @schema = '';
	IF(@table IS NULL)
		SET @table = '';
	IF(@columns IS NULL)
		SET @columns = '';
	IF(LEN(RTRIM(LTRIM(@columns))) > 0)
		SET @columns  = '[' + REPLACE(REPLACE(REPLACE(@columns,']',''),'[',''),',','],[') + ']';
		
	DECLARE
		 @continue   BIT            = 1
		,@sqlScripts NVARCHAR(MAX)  = N''
 		,@object     NVARCHAR(256)  = N''
 		,@objectId   INT            = 0
 		,@tempObject NVARCHAR(256)  = N''
 		,@allColumns NVARCHAR(1000) = N'';
 		
	SET @status     = 0;
	SET @object     = @schema + N'.' + @table;
	SET @objectId   = OBJECT_ID(@object);
	SET @tempObject = N'##' + @table + N'_tempARC';
	
	--GETTING COLUMNS IN CASE NOT ESPECIFIED
		IF(@continue = 1 AND LEN(@columns) = 0)
			BEGIN
				SET @columns = (
					SELECT
						STUFF(
							(
								SELECT ',[' + a.name + ']'
								FROM sys.columns a 
								WHERE 
									    a.object_id = @objectId
									AND a.name NOT IN ('LoadDateTime','ProcessExecutionID','RANK_NO')
								ORDER BY
									a.column_id ASC
								FOR XML PATH(''), TYPE
							).value('.', 'VARCHAR(MAX)'), 1, 1, ''
						)
				);
		   		
				IF(LEN(@columns) = 0)
					BEGIN
						SET @continue = 0;
						SET @message  = 'An error happen while trying to get the columns for the Rank';
					END
			END 
	
	--GETTING ALL COLUMNS
		IF(@continue = 1)
			BEGIN
				SET @allColumns = (
					SELECT
						STUFF(
							(
								SELECT ',[' + a.name + ']'
								FROM sys.columns a 
								WHERE 
									    a.object_id = @objectId
									AND a.name NOT IN ('RANK_NO')
								ORDER BY
									a.column_id ASC
								FOR XML PATH(''), TYPE
							).value('.', 'VARCHAR(MAX)'), 1, 1, ''
						)
				);
		   		
				IF(LEN(@allColumns) = 0)
					BEGIN
						SET @continue = 0;
						SET @message  = 'An error happen while trying to get all the columns for the Rank';
					END
			END 
	
	--VALIDATING INPUT PARAMETERS
		IF(SCHEMA_ID(@schema) IS NULL)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Schema (' + @schema + ') does not exist';
			END 
		ELSE IF(@objectId IS NULL)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Table (' + @object + ') does not exist';
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM sys.objects
				WHERE 
					    OBJECT_ID = @objectId
					AND type      = 'U'
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The object (' + @object + ') is not a valid Table';
			END
		ELSE IF(
			EXISTS(
				SELECT 1
				FROM
					dbo.udf_DelimitedSplit8K(@columns,',') a LEFT JOIN sys.columns b ON
						    b.object_id        = @objectId
						AND '[' + b.name + ']' = a.item
		   		WHERE 
		   			    b.object_id IS NULL
		   			AND a.item <> ''
		   	)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Columns specified (' + @columns + ') does not exist in the object (' + @object + ')';
			END
	
	IF(@continue = 1)
		BEGIN
			BEGIN TRANSACTION
		END
	
	--CREATING THE RANK COLUMN IN TEMP TABLE
		IF(@continue = 1)
			BEGIN
				BEGIN TRY
					IF(
						EXISTS(
							SELECT 1
							FROM sys.columns
							WHERE 
								    OBJECT_ID = @objectId 
								AND name      = 'RANK_NO'
						)
					)
						BEGIN
							SET @sqlScripts = 'ALTER TABLE ' + @object + ' DROP COLUMN RANK_NO';
							EXEC(@sqlScripts);
						END 
						
					IF(OBJECT_ID('tempdb..' + @tempObject) IS NOT NULL)
						BEGIN
							SET @sqlScripts = 'DROP TABLE ' + @tempObject;
							EXEC(@sqlScripts);
						END
					
					SET @sqlScripts = 	'SELECT 
											 a.* 
											,ROW_NUMBER() OVER ( 
												PARTITION BY 
													 ' + @columns + '
												ORDER BY 
													 ' + @columns + '
											) AS RANK_NO 
											INTO ' + @tempObject + '
										FROM 
											' + @object + ' a
										ORDER BY 
											 ' + @columns;
	 
			 		EXEC(@sqlScripts);
			 		
			 		IF(OBJECT_ID('tempdb..' + @tempObject) IS NULL)
			 			BEGIN
			 				SET @continue = 0;
							SET @message  = 'An error happen while trying to create the Rank';
			 			END
			 		ELSE IF(
			 			NOT EXISTS(
			 				SELECT 1 
							FROM 
								(
									SELECT a.row_count
									FROM sys.dm_db_partition_stats a 
									WHERE a.object_id = @objectId
								) aa INNER JOIN (
									SELECT a.row_count
									FROM tempdb.sys.dm_db_partition_stats a 
									WHERE a.object_id = OBJECT_ID('tempdb..' + @tempObject)
								) bb ON
									bb.row_count = aa.row_count
			 			)
			 		)
			 			BEGIN
			 				SET @continue = 0;
							SET @message  = 'An error happen while creating the temp table (' + @tempObject + ') does not have the same data as (' + @object + ')';
			 			END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message = 'An error occurred while trying to create the Rank';
					SET @SQL     = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
				END CATCH		
			END
	
	--BULK TEMP TABLE INTO FINAL TABLE
		IF(@continue = 1)
			BEGIN
				BEGIN TRY
					SET @sqlScripts = 'TRUNCATE TABLE ' + @object;
					EXEC(@sqlScripts);
					
					IF(
						NOT EXISTS(
							SELECT 1
							FROM sys.columns
							WHERE 
								    OBJECT_ID = @objectId 
								AND name      = 'RANK_NO'
						)
					)
						BEGIN
							SET @sqlScripts = 'ALTER TABLE ' + @object + ' ADD RANK_NO BIGINT NOT NULL';
							EXEC(@sqlScripts);
						END 
					
					SET @sqlScripts = 'INSERT INTO ' + @object + ' (' + @allColumns + ',RANK_NO) SELECT ' + @allColumns + ',RANK_NO FROM ' + @tempObject;
			 		EXEC(@sqlScripts);
			 		
			 		IF(
			 			NOT EXISTS(
			 				SELECT 1 
							FROM 
								(
									SELECT a.row_count
									FROM sys.dm_db_partition_stats a 
									WHERE a.object_id = @objectId
								) aa INNER JOIN (
									SELECT a.row_count
									FROM tempdb.sys.dm_db_partition_stats a 
									WHERE a.object_id = OBJECT_ID('tempdb..' + @tempObject)
								) bb ON
									bb.row_count = aa.row_count
			 			)
			 		)
			 			BEGIN
			 				SET @continue = 0;
							SET @message  = 'An error happen while creating the table (' + @object + ') does not have the same data as (' + @tempObject + ')';
			 			END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message = 'An error occurred while trying to create the Rank';
					SET @SQL     = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
				END CATCH	
			END
	
	IF(OBJECT_ID('tempdb..' + @tempObject) IS NOT NULL)
		BEGIN
			SET @sqlScripts = 'DROP TABLE ' + @tempObject;
			EXEC(@sqlScripts);
		END
	
	IF(@continue = 1)
		BEGIN
			SET @status = 1;
			SET @message  = 'Rank Column added';
			COMMIT TRANSACTION
		END
	ELSE
		BEGIN
			ROLLBACK TRANSACTION
		END 
END
GO
