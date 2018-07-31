CREATE PROCEDURE dbo.sp_assignParameter
	(
		 @type    VARCHAR(50)
		,@value1  VARCHAR(100)  = NULL
		,@value2  VARCHAR(100)  = NULL
		,@value3  VARCHAR(100)  = NULL
		,@value4  VARCHAR(100)  = NULL
		,@value5  VARCHAR(100)  = NULL
		,@value6  VARCHAR(100)  = NULL
		,@status  TINYINT           OUTPUT
		,@message VARCHAR(500)      OUTPUT
		,@SQL     VARCHAR(4000)     OUTPUT
	)
AS
BEGIN
	DECLARE 
		  @continue  BIT           = 1
		 ,@sqlScript NVARCHAR(MAX) = N''
		 ,@addComma  BIT           = 0;
	
	SET @message = '';
	SET @SQL     = N'';
	
	IF(OBJECT_ID('dbo.BIConfig') IS NULL)
		BEGIN
			CREATE TABLE dbo.BIConfig (
				type        VARCHAR (50) NOT NULL,
				value1      VARCHAR (100) NOT NULL,
				value2      VARCHAR (100) NOT NULL,
				value3      VARCHAR (100) NOT NULL,
				value4      VARCHAR (100) NOT NULL,
				value5      VARCHAR (100) NOT NULL,
				value6      VARCHAR (100) NOT NULL,
				description VARCHAR (300) NOT NULL,
				disabled    SMALLINT NOT NULL,
				CONSTRAINT PK_BIConfig PRIMARY KEY (type, value1, value2, value3, value4, value5, value6, disabled)
			);
		END
	
	IF(
		NOT EXISTS(
			SELECT 1
			FROM   dbo.BIConfig a
			WHERE  a.type = @type
		)
	)
		BEGIN
			SET @continue = 0;
			SET @message  = 'Error - The Type (' + @type + ') does not exist in dbo.BIConfig table'
		END
	ELSE IF(
		EXISTS(
			SELECT 1
			FROM   dbo.BIConfig a
			WHERE  
				       a.type     = @type
				   AND a.disabled = 1
		)
	)
		BEGIN
			SET @continue = 0;
			SET @message  = 'Error - The Type (' + @type + ') is disabled'
		END
	
	IF(@continue = 1)
		BEGIN
			BEGIN TRY
				SET @addComma = 0;
				
					SET @sqlScript = @sqlScript + N'UPDATE dbo.BIConfig SET ';
				IF(@value1 IS NOT NULL)
					BEGIN
						IF(@addComma = 1)
							SET @sqlScript = @sqlScript + N',';
							
						SET @sqlScript = @sqlScript + N'value1 = ''' + @value1 + ''' ';
						SET @addComma  = 1;
					END
				IF(@value2 IS NOT NULL)
					BEGIN
						IF(@addComma = 1)
							SET @sqlScript = @sqlScript + N',';
							
						SET @sqlScript = @sqlScript + N'value2 = ''' + @value2 + ''' ';
						SET @addComma  = 1;
					END
				IF(@value3 IS NOT NULL)
					BEGIN
						IF(@addComma = 1)
							SET @sqlScript = @sqlScript + N',';
							
						SET @sqlScript = @sqlScript + N'value3 = ''' + @value3 + ''' ';
						SET @addComma  = 1;
					END
				IF(@value4 IS NOT NULL)
					BEGIN
						IF(@addComma = 1)
							SET @sqlScript = @sqlScript + N',';
							
						SET @sqlScript = @sqlScript + N'value4 = ''' + @value4 + ''' ';
						SET @addComma  = 1;
					END
				IF(@value5 IS NOT NULL)
					BEGIN
						IF(@addComma = 1)
							SET @sqlScript = @sqlScript + N',';
							
						SET @sqlScript = @sqlScript + N'value5 = ''' + @value5 + ''' ';
						SET @addComma  = 1;
					END
				IF(@value6 IS NOT NULL)
					BEGIN
						IF(@addComma = 1)
							SET @sqlScript = @sqlScript + N',';
							
						SET @sqlScript = @sqlScript + N'value6 = ''' + @value6 + ''' ';
						SET @addComma  = 1;
					END
					SET @sqlScript = @sqlScript + N'WHERE type = ''' + @type + '''';
				
				EXEC(@sqlScript);
				
				SET @message = 'success';
			END TRY
			BEGIN CATCH
				SET @message = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
				SET @SQL     = @sqlScript;
			END CATCH
		END
	SET @status = @continue;
END
GO
