CREATE PROCEDURE dbo.sp_homogeniseObjectStructure
	(
		 @objectFrom                     NVARCHAR(256)
		,@objectTo                       NVARCHAR(256)
		,@addNewColumns                  TINYINT
		,@dropNonUsedColumns             TINYINT
		,@alterDataType                  TINYINT
		,@dontLoseDataWhenDataTypeChange TINYINT
		,@status                         TINYINT       OUTPUT
		,@message                        VARCHAR(500)  OUTPUT
		,@SQL                            VARCHAR(4000) OUTPUT
	)
AS
/*
  Developed by: Mauricio Rivera Senior
  Description: The following SP takes the structure of the object specified on @objectFrom and compares it with @objectTo.
  Then, according to the flags on the parameters, the @objectTo structure will be altered to looks like the @objectFrom.
*/
BEGIN
	DECLARE 
		 @continue         TINYINT       = 1
		,@sqlScripts       NVARCHAR(MAX) = ''
		,@INT              INT           = 0
		,@action           VARCHAR(50)   = ''
		,@alterTableScript NVARCHAR(500) = N'ALTER TABLE ' + @objectTo + N' ';
	
	SET @SQL     = '';
	SET @message = '';
	
	DECLARE @DHI_columnsToFix TABLE
	(
		 action  VARCHAR(50)
		,scripts NVARCHAR(4000)
	); 
	
	DECLARE @HOS_from TABLE
	(
		 columnName  NVARCHAR(128)
		,max_length  SMALLINT
		,[precision] TINYINT
		,scale       TINYINT
		,is_nullable BIT
		,dataType    NVARCHAR(128)
		,column_id   INT
	);
	
	DECLARE @HOS_to TABLE
	(
		 columnName  NVARCHAR(128)
		,max_length  SMALLINT
		,[precision] TINYINT
		,scale       TINYINT
		,is_nullable BIT
		,dataType    NVARCHAR(128)
		,column_id   INT
	);
	
	--VALIDATING INPUT PARAMETERS
		IF(
			NOT EXISTS(
					SELECT 1
					FROM sys.objects a
					WHERE 
						    a.object_id = OBJECT_ID(@objectFrom)
						AND a.type IN ('U','V')
				UNION ALL
					SELECT 1
					FROM tempdb.sys.objects a
					WHERE
						    a.object_id = OBJECT_ID('tempdb..' + @objectFrom)
						AND a.type IN ('U')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Input parameter @objectFrom (' + ISNULL(@objectFrom,'NULL') + ') is invalid.';
			END
		ELSE IF(
			NOT EXISTS(
					SELECT 1
					FROM sys.objects a
					WHERE 
						    a.object_id = OBJECT_ID(@objectTo)
						AND a.type IN ('U')
				UNION ALL
					SELECT 1
					FROM tempdb.sys.objects a
					WHERE
						    a.object_id = OBJECT_ID('tempdb..' + @objectTo)
						AND a.type IN ('U')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Input parameter @objectTo (' + ISNULL(@objectTo,'NULL') + ') is invalid.';
			END
		ELSE IF(@addNewColumns IS NULL OR @addNewColumns < 0 OR @addNewColumns > 1)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Input parameter @addNewColumns (' + ISNULL(@addNewColumns,'NULL') + ') only accept 1 or 0.';
			END
		ELSE IF(@dropNonUsedColumns IS NULL OR @dropNonUsedColumns < 0 OR @dropNonUsedColumns > 1)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Input parameter @dropNonUsedColumns (' + ISNULL(@dropNonUsedColumns,'NULL') + ') only accept 1 or 0.';
			END
		ELSE IF(@alterDataType IS NULL OR @alterDataType < 0 OR @alterDataType > 1)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Input parameter @alterDataType (' + ISNULL(@alterDataType,'NULL') + ') only accept 1 or 0.';
			END
		ELSE IF(@dontLoseDataWhenDataTypeChange IS NULL OR @dontLoseDataWhenDataTypeChange < 0 OR @dontLoseDataWhenDataTypeChange > 1)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Input parameter @dontLoseDataWhenDataTypeChange (' + ISNULL(@dontLoseDataWhenDataTypeChange,'NULL') + ') only accept 1 or 0.';
			END

	--GETTING DATA FROM SYS ELEMENTS
		IF(@continue = 1 )
			BEGIN
				--GETTING FROM OBJECT DATA
					IF(
						NOT EXISTS(
							SELECT 1
							FROM tempdb.sys.objects a
							WHERE
								    a.object_id = OBJECT_ID('tempdb..' + @objectFrom)
								AND a.type IN ('U')
						)
					)
						BEGIN
							--THE FROM OBJECT IS NOT A TEMP TABLE
								INSERT INTO @HOS_from
									SELECT
										 a1.name AS columnName
										,a1.max_length
										,a1.precision
										,a1.scale
										,a1.is_nullable
										,b1.name AS dataType
										,a1.column_id
									FROM
										sys.columns a1 INNER JOIN sys.types b1 ON
											    b1.user_type_id   = a1.user_type_id
											AND b1.system_type_id = a1.system_type_id
									WHERE
										a1.object_id = OBJECT_ID(@objectFrom);
						END
					ELSE
						BEGIN
							--THE FROM OBJECT IS A TEMP TABLE
								INSERT INTO @HOS_from
									SELECT
										 a1.name AS columnName
										,a1.max_length
										,a1.precision
										,a1.scale
										,a1.is_nullable
										,b1.name AS dataType
										,a1.column_id
									FROM
										tempdb.sys.columns a1 INNER JOIN tempdb.sys.types b1 ON
											    b1.user_type_id   = a1.user_type_id
											AND b1.system_type_id = a1.system_type_id
									WHERE
										a1.object_id = OBJECT_ID('tempdb..' + @objectFrom);
						END
				
				--GETTING TO OBJECT DATA
					IF(
						NOT EXISTS(
							SELECT 1
							FROM tempdb.sys.objects a
							WHERE
								    a.object_id = OBJECT_ID('tempdb..' + @objectTo)
								AND a.type IN ('U')
						)
					)
						BEGIN
							INSERT INTO @HOS_to
								SELECT
									 a1.name AS columnName
									,a1.max_length
									,a1.precision
									,a1.scale
									,a1.is_nullable
									,b1.name AS dataType
									,a1.column_id
								FROM
									sys.columns a1 INNER JOIN sys.types b1 ON
										    b1.user_type_id  = a1.user_type_id 
										AND b1.system_type_id = a1.system_type_id
								WHERE
									a1.object_id = OBJECT_ID(@objectTo);
						END
					ELSE
						BEGIN
							INSERT INTO @HOS_to
								SELECT
									 a1.name AS columnName
									,a1.max_length
									,a1.precision
									,a1.scale
									,a1.is_nullable
									,b1.name AS dataType
									,a1.column_id
								FROM
									tempdb.sys.columns a1 INNER JOIN tempdb.sys.types b1 ON
										    b1.user_type_id  = a1.user_type_id 
										AND b1.system_type_id = a1.system_type_id
								WHERE
									a1.object_id = OBJECT_ID('tempdb..' + @objectTo);
						END
			END

	--GENERATING ALTER SCRIPTS
		IF(@continue = 1)
			BEGIN					
				INSERT INTO @DHI_columnsToFix
					SELECT
						action,
						CASE
							WHEN (action = 'DROP') THEN @alterTableScript + N' DROP COLUMN [' + columnName + N'] '
							WHEN (action = 'ADD')  THEN @alterTableScript + N' ADD [' + columnName + N'] ' + 
								CASE
									WHEN (precision_source = 0) THEN
										CASE
											WHEN (dataType_source IN (N'nvarchar',N'nchar',N'ntext')) THEN 
												UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_source / 2 AS NVARCHAR(5)) + N')'
											ELSE
												UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_source AS NVARCHAR(5)) + N')'
										END
									ELSE
										CASE
											WHEN (dataType_source IN (N'decimal',N'numeric')) THEN N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(precision_source AS NVARCHAR(4)) + N',' + CAST(scale_source AS NVARCHAR(5)) + N')' 
											ELSE N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT)
										END
								END
								+ N' ' + CASE
									WHEN (is_nullable_source = 1) THEN N'NULL'
									ELSE N'NOT NULL'
								END
							WHEN (action = N'ALTER') THEN
								CASE
									WHEN (@dontLoseDataWhenDataTypeChange = 1) THEN
										CASE
											WHEN (
												    max_length_source >= max_length_destination 
												AND ( (precision_source - scale_source) >= (precision_destination - scale_destination) ) 
												AND scale_source >= scale_destination
												AND (
													   is_nullable_source = is_nullable_destination
													OR (
														    is_nullable_source      = 1
														AND is_nullable_destination = 0
													)
												)
											) THEN
												@alterTableScript + N' ALTER COLUMN [' + columnName + N'] ' + 
												CASE 
													WHEN (precision_source = 0) THEN 
														CASE
															WHEN (dataType_source IN (N'nvarchar',N'nchar',N'ntext')) THEN N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_source / 2 AS NVARCHAR(5)) + N')' 
															ELSE N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_source AS NVARCHAR(5)) + N')'
														END
													ELSE 
														CASE
															WHEN (dataType_source IN (N'decimal',N'numeric')) THEN N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(precision_source AS NVARCHAR(4)) + N',' + CAST(scale_source AS NVARCHAR(5)) + N')' 
															ELSE N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT)
														END
												END + 
												CASE
													WHEN (is_nullable_source = 1) THEN N' NULL'
													ELSE N' NOT NULL'
												END
											ELSE 
												/*N'Error: ' + @alterTableScript + N' ALTER COLUMN ([' + columnName + N'] ' + 
												CASE 
													WHEN (precision_source = 0) THEN 
														CASE
															WHEN (dataType_source IN (N'nvarchar',N'nchar',N'ntext')) THEN N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_source / 2 AS NVARCHAR(5)) + N')' 
															ELSE N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_source AS NVARCHAR(5)) + N')'
														END
													ELSE 
														CASE
															WHEN (dataType_source IN (N'decimal',N'numeric')) THEN N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(precision_source AS NVARCHAR(4)) + N',' + CAST(scale_source AS NVARCHAR(5)) + N')' 
															ELSE N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT)
														END
												END + 
												CASE
													WHEN (is_nullable_source = 1) THEN N' NULL'
													ELSE N' NOT NULL'
												END + N') | From: ' + 
												CASE 
													WHEN (precision_destination = 0) THEN 
														CASE
															WHEN (dataType_destination IN (N'nvarchar',N'nchar',N'ntext')) THEN N' ' + UPPER(dataType_destination COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_destination / 2 AS NVARCHAR(5)) + N')' 
															ELSE N' ' + UPPER(dataType_destination COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_destination AS NVARCHAR(5)) + N')'
														END
													ELSE 
														CASE
															WHEN (dataType_destination IN (N'decimal',N'numeric')) THEN N' ' + UPPER(dataType_destination COLLATE DATABASE_DEFAULT) + N'(' + CAST(precision_destination AS NVARCHAR(4)) + N',' + CAST(scale_destination AS NVARCHAR(5)) + N')' 
															ELSE N' ' + UPPER(dataType_destination COLLATE DATABASE_DEFAULT)
														END
												END + 
												CASE
													WHEN (is_nullable_destination = 1) THEN N' NULL'
													ELSE N' NOT NULL'
												END */
												''
										END
									ELSE
										@alterTableScript + N'  ALTER COLUMN  [' + columnName + N'] ' + CASE 
											WHEN (precision_source = 0) THEN 
												CASE
													WHEN (dataType_source IN (N'nvarchar',N'nchar',N'ntext')) THEN N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_source / 2 AS NVARCHAR(5)) + N')'
													ELSE N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(max_length_source AS NVARCHAR(5)) + N')'
												END
											ELSE 
												CASE
													WHEN (dataType_source IN (N'decimal',N'numeric')) THEN N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT) + N'(' + CAST(precision_source AS NVARCHAR(4)) + N',' + CAST(scale_source AS NVARCHAR(5)) + N')' 
													ELSE N' ' + UPPER(dataType_source COLLATE DATABASE_DEFAULT)
												END
										END 
										+ CASE
											WHEN (is_nullable_source = 1) THEN N' NULL'
											ELSE N' NOT NULL'
										END
								END
						END AS scripts
					FROM
						(
							SELECT
								CASE
									WHEN (a2.columnName COLLATE DATABASE_DEFAULT = b2.columnName COLLATE DATABASE_DEFAULT) THEN 
										CASE
											WHEN (
												    a2.max_length  = b2.max_length 
												AND a2.precision   = b2.precision   
												AND a2.scale        = b2.scale       
												AND a2.is_nullable  = b2.is_nullable 
											) THEN 'NO-CHANGE'
											ELSE 'ALTER'
										END
									ELSE
										CASE
											WHEN (a2.columnName COLLATE DATABASE_DEFAULT IS NULL) THEN 'DROP'
											ELSE 'ADD'
										END
								END AS action
								,COALESCE(a2.columnName  COLLATE DATABASE_DEFAULT, b2.columnName  COLLATE DATABASE_DEFAULT) AS columnName
								,COALESCE(a2.max_length                          , b2.max_length                          ) AS max_length_source
								,COALESCE(a2.precision                           , b2.precision                           ) AS precision_source
								,COALESCE(a2.scale                               , b2.scale                               ) AS scale_source
								,COALESCE(a2.is_nullable                         , b2.is_nullable                         ) AS is_nullable_source
								,COALESCE(a2.dataType    COLLATE DATABASE_DEFAULT, b2.dataType    COLLATE DATABASE_DEFAULT) AS dataType_source
								,COALESCE(b2.max_length                          , a2.max_length                          ) AS max_length_destination
								,COALESCE(b2.precision                           , a2.precision                           ) AS precision_destination
								,COALESCE(b2.scale                               , a2.scale                               ) AS scale_destination
								,COALESCE(b2.is_nullable                         , a2.is_nullable                         ) AS is_nullable_destination
								,COALESCE(b2.dataType    COLLATE DATABASE_DEFAULT, a2.dataType    COLLATE DATABASE_DEFAULT) AS dataType_destination
								,COALESCE(b2.column_id                           , a2.column_id                           ) AS column_id
							FROM
								@HOS_from a2 FULL OUTER JOIN @HOS_to b2 ON
									b2.columnName COLLATE DATABASE_DEFAULT = a2.columnName COLLATE DATABASE_DEFAULT
						) a3
					WHERE
						(
							    action              = 'DROP'
							AND @dropNonUsedColumns = 1
						)
						OR (
							action             = 'ADD'
							AND @addNewColumns = 1
						)
						OR (
							action             = 'ALTER'
							AND @alterDataType = 1
						);		 			   
			END

/*#####################################################################################################################################################################################
##                                                                 S  R  I  P  T  S        E  X  E  C  U  T  I  O  N                                                                 ##
#####################################################################################################################################################################################*/
	IF(@continue = 1 )
		BEGIN
			IF(
				NOT EXISTS(
					SELECT 1
					FROM @DHI_columnsToFix
					WHERE scripts <> ''
				)
			)
				BEGIN
					SET @message = 'No differences found';
				END
			ELSE
				BEGIN
					--CHECK IF THERE IS A CONVERTION ERROR
						IF(
							EXISTS(
								SELECT 1
								FROM @DHI_columnsToFix a
								WHERE a.scripts LIKE '%ERROR%'
							)
						)
							BEGIN
								SET @continue = 0;
								SET @message  = 'Convertion error Found';
								SET @SQL      = (
									SELECT 
										STUFF(
											( 
												SELECT ' || ' + scripts 
												FROM @DHI_columnsToFix
												FOR XML PATH(''), TYPE
											).value('.', 'VARCHAR(MAX)'), 1, 4, ''
										)
								);
							END

					--EXECUTING SCRIPTS
						IF(@continue = 1)
							BEGIN
								BEGIN TRANSACTION
								
									IF (SELECT CURSOR_STATUS('global','hos_cursor')) >= -1
										BEGIN
											DEALLOCATE hos_cursor;
										END
									
									DECLARE hos_cursor CURSOR LOCAL FOR						
										SELECT 
											 a.action
											,a.scripts
										FROM @DHI_columnsToFix a
										WHERE scripts <> '';
									
									OPEN hos_cursor;
									
									FETCH NEXT FROM hos_cursor INTO @action,@sqlScripts;
									
									WHILE (@@FETCH_STATUS = 0 AND @continue = 1)
										BEGIN
											BEGIN TRY
												EXEC(@sqlScripts);
											END TRY
											BEGIN CATCH
												SET @continue = 0;
												SET @message  = 'An error occurred while trying to ' + @action + ' a Column in the Table ' + @objectTo;
												SET @SQL      = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'') + ' SCRIPT: ' + @sqlScripts;
											END CATCH			   
											FETCH NEXT FROM hos_cursor INTO @action,@sqlScripts;
										END
									
									CLOSE hos_cursor;
									
									IF (SELECT CURSOR_STATUS('global','hos_cursor')) >= -1
										BEGIN
											DEALLOCATE hos_cursor;
										END
								
								IF(@continue = 1)
									BEGIN
										COMMIT TRANSACTION
										SELECT @INT = COUNT(*) FROM @DHI_columnsToFix;
										SET @message  = '(' + CAST(@INT AS VARCHAR(3)) + ') differences processed successfully';
										
									END	
								ELSE
									BEGIN
										ROLLBACK TRANSACTION
									END
							END
				END
		END
	
	SET @status = @continue;
END
GO
