CREATE PROCEDURE dbo.sp_RBIL_processFinalTable 
	(
		 @CURSchema            NVARCHAR(128) = NULL
		,@CURObjectName        NVARCHAR(128) = NULL
		,@HSTSchema            NVARCHAR(128) = NULL
		,@HSTObjectName        NVARCHAR(128) = NULL
		,@destinationSchema    NVARCHAR(128) = NULL
		,@destinationTableName NVARCHAR(128) = NULL
		,@beginDateColumnName  NVARCHAR(256) = NULL
		,@endDateColumnName    NVARCHAR(256) = NULL
		,@asAtDate             DATETIME      = NULL 
		,@status               TINYINT              OUTPUT
		,@message              VARCHAR(1000)        OUTPUT
		,@SQL                  VARCHAR(MAX)         OUTPUT
		,@SQL2                 VARCHAR(MAX)         OUTPUT
		,@SQL3                 VARCHAR(MAX)         OUTPUT
	)
AS
/*
	Developed by: Mauricio Rivera
	Date: 8 June 2018
	
	MODIFICATIONS
		
*/
BEGIN
	--Transforming input parameter from NULL to default value
		IF(@CURSchema IS NULL)
			SET @CURSchema = '';
		
		IF(@CURObjectName IS NULL)
			SET @CURObjectName = '';
		
		IF(@HSTSchema IS NULL)
			SET @HSTSchema = '';
		
		IF(@HSTObjectName IS NULL)
			SET @HSTObjectName = '';
		
		IF(@destinationSchema IS NULL)
			SET @destinationSchema = '';
		
		IF(@destinationTableName IS NULL)
			SET @destinationTableName = '';
		
		IF(@beginDateColumnName IS NULL)
			SET @beginDateColumnName = '';
			
		IF(@endDateColumnName IS NULL)
			SET @endDateColumnName = '';
		
		IF(@asAtDate IS NULL)
			SET @asAtDate = '';
		
		SET @message = '';
		SET @SQL     = '';
		
	DECLARE
	--PROCESS FLOW VARIABLES
		 @sqlScript1                NVARCHAR(MAX)   = N''
		,@sqlScript2                NVARCHAR(MAX)   = N''
		,@sqlScript3                NVARCHAR(MAX)   = N''
		,@INT                       INT             = 0
		,@NVARCHAR                  NVARCHAR(1000)  = N''
	--FLAGS VARIABLES
		,@continue                  TINYINT         = 1
	--GENERAL VARIABLES
		,@CURObjectFull             NVARCHAR(256)   = N''
		,@CURObjectId               INT             = 0
		,@HSTObjectFull             NVARCHAR(256)   = N''
		,@HSTObjectId               INT             = 0
		,@destinationTableFull      NVARCHAR(256)   = N''
		,@destinationTableObjectId  INT             = 0
		,@dataTempTable             NVARCHAR(256)   = N''
		,@asAtDateVarchar           NVARCHAR(50)    = N''
		,@columns                   NVARCHAR(4000)  = N''
		
	--INITIALIZING VARIABLES
		SET @CURObjectFull             = @CURSchema + N'.' + @CURObjectName;
		SET @CURObjectId               = OBJECT_ID(@CURObjectFull);
		SET @HSTObjectFull             = @HSTSchema + N'.' + @HSTObjectName;
		SET @HSTObjectId               = OBJECT_ID(@HSTObjectFull);
		SET @destinationTableFull      = @destinationSchema + N'.' + @destinationTableName;
		SET @destinationTableObjectId  = OBJECT_ID(@destinationTableFull);
		SET @dataTempTable             = @destinationSchema + N'.' + @destinationTableName + N'_TEMPDATA';
		SET @asAtDateVarchar           = CONVERT(NVARCHAR,@asAtDate,100);
	
	--VALIDATING INPUT PARAMETERS
		IF(SCHEMA_ID(@CURSchema) IS NULL)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The input parameter @CURSchema (' + @CURSchema + ') is not valid';
			END
		ELSE IF(@CURObjectId IS NULL)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The input parameter @CURObjectName (' + @CURObjectName + ') is not valid';
			END
		ELSE IF(
			EXISTS(
				SELECT 1
				FROM sys.objects a
				WHERE 
					    a.object_id = @CURObjectId
					AND a.type NOT IN ('U')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Source Table (' + @CURObjectFull + ') is not valid. It must be a valid Table';
			END
		ELSE IF(SCHEMA_ID(@HSTSchema) IS NULL)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The input parameter @HSTSchema (' + @HSTSchema + ') is not valid';
			END
		ELSE IF(@HSTObjectId IS NULL)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The input parameter @HSTObjectName (' + @HSTObjectName + ') is not valid';
			END
		ELSE IF(
			EXISTS(
				SELECT 1
				FROM sys.objects a
				WHERE 
					    a.object_id = @HSTObjectId
					AND a.type NOT IN ('U')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The History Table (' + @HSTObjectFull + ') is not valid. It must be a valid Table';
			END
		ELSE IF(SCHEMA_ID(@destinationSchema) IS NULL)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The input parameter @destinationSchema (' + @destinationSchema + ') is not valid';
			END
		ELSE IF(@CURObjectFull = @destinationTableFull)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Source object (' + @CURObjectFull + ') and the Destination Table (' + @destinationTableFull + ') must not be the same object';
			END
		ELSE IF(@HSTObjectFull = @destinationTableFull)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The History object (' + @HSTObjectFull + ') and the Destination Table (' + @destinationTableFull + ') must not be the same object';
			END
		ELSE IF(@CURObjectFull = @HSTObjectFull)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The Source Table (' + @CURObjectFull + ') and the History Table (' + @HSTObjectFull + ') must not be the same object';
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM 
					sys.columns a INNER JOIN sys.types b ON
						    b.system_type_id = a.system_type_id
						AND b.user_type_id   = a.user_type_id
				WHERE 
					    a.object_id   = @CURObjectId
					AND a.name        = @beginDateColumnName
					AND UPPER(b.name) IN ('DATETIME','DATETIME2')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The BeginDateColumn (' + @beginDateColumnName + ') does not exist in the Source Table (' + @CURObjectFull + ') or have not a valid data type (DATETIME, DATETIME2)';
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM 
					sys.columns a INNER JOIN sys.types b ON
						    b.system_type_id = a.system_type_id
						AND b.user_type_id   = a.user_type_id
				WHERE 
					    a.object_id   = @HSTObjectId
					AND a.name        = @beginDateColumnName
					AND UPPER(b.name) IN ('DATETIME','DATETIME2')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The BeginDateColumn (' + @beginDateColumnName + ') does not exist in the History Table (' + @HSTObjectFull + ') or have not a valid data type (DATETIME, DATETIME2)';
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM 
					sys.columns a INNER JOIN sys.types b ON
						    b.system_type_id = a.system_type_id
						AND b.user_type_id   = a.user_type_id
				WHERE 
					    a.object_id   = @HSTObjectId
					AND a.name        = @endDateColumnName
					AND UPPER(b.name) IN ('DATETIME','DATETIME2')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message  = 'The EndDateColumn (' + @endDateColumnName + ') does not exist in the History Table (' + @HSTObjectFull + ') or have not a valid data type (DATETIME, DATETIME2)';
			END
		ELSE IF(@asAtDate > GETDATE())
			BEGIN
				SET @continue = 0;
				SET @message  = 'The AsAtDate (' + CONVERT(VARCHAR(50),@asAtDate,100) + ') cannot be greater than the current date and time (' + CONVERT(VARCHAR(50),GETDATE(),100) + ')';
			END
	
	--GETTING THE COLUMNS
		IF(@continue = 1)
			BEGIN
				BEGIN TRY
					SET @columns = (
						SELECT
							STUFF(
								(
									SELECT ',[' + a.name + ']'
									FROM 
										sys.columns a INNER JOIN sys.columns b ON
											    a.object_id = @CURObjectId
											AND b.object_id = @HSTObjectId
											AND b.name      = a.name
									ORDER BY
										a.column_id ASC
									FOR XML PATH(''), TYPE
								).value('.', 'VARCHAR(MAX)'), 1, 1, ''
							)
					);
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = '';
				END CATCH
			END
	
	--DROPPING TEMP TABLE
		IF(@continue = 1 AND OBJECT_ID(@dataTempTable) IS NOT NULL)
			BEGIN
				BEGIN TRY
					SET @sqlScript1 = N'DROP TABLE ' + @dataTempTable;
					EXEC(@sqlScript1);
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = ISNULL(@sqlScript1,'');
				END CATCH
			END
	
	--GENERATING DATA
		IF(@continue = 1)
			BEGIN				
				BEGIN TRY
					SET @sqlScript1 = 'SELECT
' + @columns + '
INTO ' + @dataTempTable + '
FROM
	(';
					SET @sqlScript2 = ' SELECT
				' + @columns + '
			FROM 
				' + @CURObjectFull + ' 
			WHERE
				' + @beginDateColumnName + ' <= ''' + @asAtDateVarchar + '''';
					SET @sqlScript3 = ' UNION ALL
			SELECT
				' + @columns + '
			FROM
				' + @HSTObjectFull + ' 
			WHERE
				' + @beginDateColumnName + ' <= ''' + @asAtDateVarchar + '''
				AND ' + @endDateColumnName + ' > ''' + @asAtDateVarchar + '''
	) a';
					SET @INT = 0;
					
					EXEC(@sqlScript1 + @sqlScript2 + @sqlScript3);

					SET @INT = @@ROWCOUNT;
					
					IF(@INT = 0)
						BEGIN
							SET @continue = 0;
							SET @message  = 'No data found to be processed';
							SET @SQL      = ISNULL(@sqlScript1,'');
							SET @SQL2     = ISNULL(@sqlScript2,'');
							SET @SQL3     = ISNULL(@sqlScript3,'');
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = ISNULL(@sqlScript1,'');
					SET @SQL2     = ISNULL(@sqlScript2,'');
					SET @SQL3     = ISNULL(@sqlScript3,'');
				END CATCH
			END

	--DROPPING DESTINATION TABLE
		IF(@continue = 1 AND OBJECT_ID(@destinationTableFull) IS NOT NULL)
			BEGIN				
				BEGIN TRY
					SET @sqlScript1 = 'DROP TABLE ' + @destinationTableFull;
					EXEC(@sqlScript1);
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = ISNULL(@sqlScript1,'');
				END CATCH
			END
	
	--CREATING DESTINATION TABLE
		IF(@continue = 1)
			BEGIN				
				BEGIN TRY
					SET @sqlScript1 = 'EXEC sp_rename ''' + @dataTempTable + ''',''' + @destinationTableName + '''';
					EXEC(@sqlScript1);
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					SET @message  = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
					SET @SQL      = ISNULL(@sqlScript1,'');
				END CATCH
			END
	
	SET @status = @continue;
END
GO
