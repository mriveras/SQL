CREATE PROCEDURE dbo.sp_manageIndexes
	(
		 @action    TINYINT              = 1    --(1) = Create | (2) = Drop | (3) = Status
		,@type      TINYINT              = 2    --(1) = CLUSTERED INDEX | (2) = NON-CLUSTERED INDEX || Mandatory for Create
		,@indexName NVARCHAR(128)        = NULL --Always Mandatory
		,@schema    NVARCHAR(128)        = NULL --Mandatory for Create. As well for Drop if @indexName is not specified
		,@table     NVARCHAR(128)        = NULL --Mandatory for Create. As well for Drop if @indexName is not specified
		,@columns   NVARCHAR(500)        = NULL --Mandatory for Create. As well for Drop if @indexName is not specified
		,@status    TINYINT       OUTPUT
		,@message   VARCHAR(2000) OUTPUT
		,@SQL       VARCHAR(1000) OUTPUT
	)
AS
BEGIN
	IF(@type IS NULL)
		SET @type = 2;
	IF(@indexName IS NULL)
		SET @indexName = '';
	IF(@schema IS NULL)
		SET @schema = '';
	IF(@table IS NULL)
		SET @table = '';
	IF(@columns IS NULL)
		SET @columns = '';
	IF(@action IS NULL)
		SET @action = 1;
	
	SET @indexName = RTRIM(LTRIM(@indexName));
	SET @schema    = RTRIM(LTRIM(@schema));
	SET @table     = RTRIM(LTRIM(@table));
	SET @columns   = RTRIM(LTRIM(@columns));
	SET @SQL       = '';
		
	DECLARE 
	--PROCESS FLOW
		 @continue                              BIT           = 1
		,@sqlScripts                            NVARCHAR(MAX) = ''
		,@INT                                   INT           = 0
	--FLAGS
		,@checkType                             BIT           = 0
		,@checkIndexNameExists                  BIT           = 0
		,@checkIndexNameNotExists               BIT           = 0
		,@checkSchema                           BIT           = 0
		,@checkTable                            BIT           = 0
		,@checkColumns                          BIT           = 0
		,@checkSchemaTableColumnsIndexNotExist  BIT           = 0
		,@checkIndexNameSchemaTableExist        BIT           = 0
		
	--VALIDATING INPUT PARAMETER @action
		IF(@action NOT IN (1,2,3))
			BEGIN
				SET @continue = 0;
				SET @status   = 0;
				SET @message  = 'The input parameter @action (' + @action + ') only accept 1, 2 or 3 as a value. (1) = Create Index | (2) = Drop Index | (3) = Status of the Index';
			END
		ELSE IF(LEN(@indexName) = 0)
			BEGIN
				SET @continue = 0;
				SET @status   = 0;
				SET @message  = 'The input parameter @indexName (' + @indexName + ') is Mandatory';
			END
			
	 --SELECT WHICH VALIDATIONS ARE REQUIRED	
		IF(@continue = 1 AND @action = 1)--When @action is CREATE INDEX
			BEGIN
				SET @checkType                            = 1;
				SET @checkIndexNameNotExists              = 1;
				SET @checkSchema                          = 1;
				SET @checkTable                           = 1;
				SET @checkColumns                         = 1;
				SET @checkSchemaTableColumnsIndexNotExist = 1;
			END
		ELSE IF(@continue = 1 AND @action = 2) --When @action is DROP INDEX
			BEGIN
				SET @checkSchema                    = 1;
				SET @checkTable                     = 1;
				SET @checkIndexNameSchemaTableExist = 1;
			END
		ELSE IF(@continue = 1 AND @action = 3) --When @action is INDEX STATUS
			BEGIN
				SET @checkSchema  = 1;
				SET @checkTable   = 1;
				SET @checkColumns = 1;
			END
			
	--VALIDATIONS: (CHEKING INDEX TYPE)
		IF(@continue = 1 AND @checkType = 1)
			BEGIN
				IF(@type NOT IN (1,2))
					BEGIN
						SET @continue = 0;
						SET @status   = 0;
						SET @message  = 'The input parameter @type (' + @type + ') only accept 1 or 2 as a value. (1) = Clustered Index | (2) = Non-Clustered Index';
					END
			END
	--VALIDATIONS: (CHEKING INDEX NAME DOES NOT EXIST)
		IF(@continue = 1 AND @checkIndexNameNotExists = 1)
			BEGIN
				IF(
					EXISTS(
						SELECT 1
						FROM sys.indexes
						WHERE name = @indexName
					)
				)
					BEGIN
						SET @continue = 0;
						SET @status   = 1;
						SET @message  = 'Index Found';
					END
			END
	--VALIDATIONS: (CHEKING SCHEMA)
		IF(@continue = 1 AND @checkSchema = 1)
			BEGIN
				IF(LEN(@schema) = 0)
					BEGIN
						SET @continue = 0;
						SET @status   = 0;
						SET @message  = 'The input parameter @schema (' + @schema + ') is required';
					END
				ELSE IF(SCHEMA_ID(@schema) IS NULL)
					BEGIN
						SET @continue = 0;
						SET @status   = 0;
						SET @message  = 'The Schema (' + @schema + ') does not exist';
					END
			END
	--VALIDATIONS: (CHEKING TABLE)
		IF(@continue = 1 AND @checkTable = 1)
			BEGIN
				IF(LEN(@table) = 0)
					BEGIN
						SET @continue = 0;
						SET @status   = 0;
						SET @message  = 'The input parameter @table (' + @table + ') is required';
					END
				ELSE IF(OBJECT_ID(@schema + '.' + @table) IS NULL)
					BEGIN
						SET @continue = 0;
						SET @status   = 0;
						SET @message  = 'The Table (' + @schema + '.' + @table + ') does not exist';
					END
			END
	--VALIDATIONS: (CHEKING COLUMNS)
		IF(@continue = 1 AND @checkColumns = 1)
			BEGIN
				IF(LEN(@columns) = 0)
					BEGIN
						SET @continue = 0;
						SET @status   = 0;
						SET @message  = 'The input parameter @columns (' + @columns + ') is required';
					END
				ELSE IF(
					EXISTS(
						SELECT 1
						FROM
							dbo.udf_DelimitedSplit8K(@columns,',') a LEFT JOIN sys.columns b ON
								    b.object_id = OBJECT_ID(@schema + N'.' + @table)
								AND b.name = a.item
				   		WHERE b.object_id IS NULL
				   	)
				)
					BEGIN
						SET @continue = 0;
						SET @status   = 0;
						SET @message  = 'The Columns specified (' + @columns + ') does not exist in the object (' + @schema + '.' + @table + ')';
					END
			END
	--VALIDATIONS: (CHEKING RELATION BETWEEN INDEX NAME, TABLE & SCHEMA)
		IF(@continue = 1 AND @checkIndexNameSchemaTableExist = 1)
			BEGIN
				IF(
					NOT EXISTS(
						SELECT 1
						FROM 
							sys.indexes i INNER JOIN sys.objects o ON
								o.object_id = i.object_id
							INNER JOIN sys.schemas s ON
								s.[schema_id] = o.[schema_id]
						WHERE
							    i.index_id > 0
							AND i.name     = @indexName
							AND s.name     = @schema
							AND o.name     = @table
					)
				)
					BEGIN
						SET @continue = 0;
						IF(@action = 2)--DROP INDEX
							BEGIN
								SET @status   = 1;
								SET @message  = 'Index not found';
							END
						ELSE
							BEGIN
								SET @status   = 0;
								SET @message  = 'Index not found under the Name (' + @indexName + ') in the Table (' + @schema + '.' + @table + ')';
							END
					END
			END
	--VALIDATIONS: (CHEKING SCHEMA, TABLE, COLUMNS & INDEX NAME DOES NOT EXIST)
		IF(@continue = 1 AND @checkSchemaTableColumnsIndexNotExist = 1)
			BEGIN
				IF(
					EXISTS(
						SELECT 1
						FROM
							(
								SELECT 
									 i.name AS indexName
									,s.name AS schemaName
									,o.name AS tableName
									,(
										STUFF(
											(
												SELECT
													',' + c2.name
												FROM
													sys.index_columns ic2 INNER JOIN sys.columns c2 ON
														c2.object_id = ic2.object_id
														AND c2.column_id = ic2.column_id
												WHERE
													ic2.object_id = i.object_id
													AND ic2.index_id = i.index_id
												ORDER BY
													ic2.index_column_id ASC 
												FOR XML PATH(''), TYPE
											).value('.', 'VARCHAR(MAX)'), 1, 1, ''
										)
									) AS IndexColumns
								FROM 
									sys.indexes i INNER JOIN sys.objects o ON
										o.object_id = i.object_id
									INNER JOIN sys.schemas s ON
										s.[schema_id] = o.[schema_id]
								WHERE
									    i.index_id > 0
									AND s.name     = @schema
									AND o.name     = @table
							) aa
						WHERE
							aa.IndexColumns = @columns
					)
				)
					BEGIN
						SET @continue = 0;
						SET @status   = 1;
						SET @message  = 'Index Found';
					END
			END
	
	--PROCESSING THE INDEX
		IF(@continue = 1)--ALL VALIDATIONS PASSED
			BEGIN
				IF(@action = 1) --CREATE INDEX
					BEGIN
						BEGIN TRY							
							IF(@type = 1)--CLUSTERED INDEX
								BEGIN
									SET @sqlScripts = N'CREATE CLUSTERED INDEX ';
								END
							ELSE --NON-CLUSTERED INDEX
								BEGIN
									SET @sqlScripts = N'CREATE NONCLUSTERED INDEX ';
								END
							
							SET @sqlScripts = @sqlScripts + @indexName + N' ON ' + @schema + N'.' + @table + N' (' + @columns + ')';
											
							EXEC(@sqlScripts);
							
							IF(
								EXISTS(									
									SELECT 1
									FROM
										(
											SELECT 
												 i.name AS indexName
												,s.name AS schemaName
												,o.name AS tableName
												,(
													STUFF(
														(
															SELECT
																',' + c2.name
															FROM
																sys.index_columns ic2 INNER JOIN sys.columns c2 ON
																	c2.object_id = ic2.object_id
																	AND c2.column_id = ic2.column_id
															WHERE
																ic2.object_id = i.object_id
																AND ic2.index_id = i.index_id
															ORDER BY
																ic2.index_column_id ASC 
															FOR XML PATH(''), TYPE
														).value('.', 'VARCHAR(MAX)'), 1, 1, ''
													)
												) AS IndexColumns
											FROM 
												sys.indexes i INNER JOIN sys.objects o ON
													o.object_id = i.object_id
												INNER JOIN sys.schemas s ON
													s.[schema_id] = o.[schema_id]
											WHERE
												    i.index_id > 0
												AND s.name     = @schema
												AND o.name     = @table
										) aa
									WHERE
										aa.IndexColumns = @columns
								)
							)
								BEGIN
									SET @status  = 1;
									SET @message = 'Index created Successfully';
								END
							ELSE
								BEGIN
									SET @status  = 0;
									SET @message = 'Index creation fail';
								END 
						END TRY
						BEGIN CATCH
							SET @status  = 0;
							SET @message = 'An error occurred while trying to CREATE INDEX';
							SET @SQL     = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');;
						END CATCH
					END
				ELSE IF(@action = 2) --DROP INDEX
					BEGIN
						BEGIN TRY
							SET @sqlScripts = N'DROP INDEX ' + @indexName + N' ON ' + @schema +N'.' + @table;
							
							EXEC(@sqlScripts);
							
							IF(
								NOT EXISTS(
									SELECT 1
									FROM sys.indexes a
									WHERE a.name = @indexName
								)
							)
								BEGIN
									SET @status  = 1;
									SET @message = 'Index Dropped Successfully';
								END
							ELSE
								BEGIN
									SET @status  = 0;
									SET @message = 'Drop Index fail';
								END
						END TRY
						BEGIN CATCH
							SET @status  = 0;
							SET @message = 'An error occurred while trying to DROP INDEX';
							SET @SQL     = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');;
						END CATCH
					END
				ELSE IF(@action = 3) --Status
					BEGIN
						IF(
							EXISTS(
								SELECT 1
								FROM
									(
										SELECT 
											 i.name AS indexName
											,s.name AS schemaName
											,o.name AS tableName
											,(
												STUFF(
													(
														SELECT
															',' + c2.name
														FROM
															sys.index_columns ic2 INNER JOIN sys.columns c2 ON
																c2.object_id = ic2.object_id
																AND c2.column_id = ic2.column_id
														WHERE
															ic2.object_id = i.object_id
															AND ic2.index_id = i.index_id
														ORDER BY
															ic2.index_column_id ASC 
														FOR XML PATH(''), TYPE
													).value('.', 'VARCHAR(MAX)'), 1, 1, ''
												)
											) AS IndexColumns
										FROM 
											sys.indexes i INNER JOIN sys.objects o ON
												o.object_id = i.object_id
											INNER JOIN sys.schemas s ON
												s.[schema_id] = o.[schema_id]
										WHERE
											    i.index_id > 0
											AND s.name     = @schema
											AND o.name     = @table
									) aa
								WHERE
									aa.IndexColumns = @columns
							)
						)
							BEGIN
								SET @status  = 1;
								SET @message = 'Index Found';
							END
						ELSE
							BEGIN
								SET @status  = 1;
								SET @message = 'Index Not Found';
							END
					END
			END
END
GO
