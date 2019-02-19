IF OBJECT_ID (N'Config.sp_waitForMe') IS NOT NULL
	DROP PROCEDURE Config.sp_waitForMe
GO

CREATE PROCEDURE Config.sp_waitForMe
	(
		 @serverName     NVARCHAR(128)
		,@dataBaseName   NVARCHAR(128)
		,@schemaName     NVARCHAR(128)
		,@tableName      NVARCHAR(128)
		,@columnName     NVARCHAR(128)
		,@whereClause    NVARCHAR(500)
		,@waitValue      VARCHAR(50)
		,@checkEveryUnit VARCHAR(10)
		,@checkEveryTime INT
		,@result         BIT OUTPUT
	)
AS
BEGIN
	DECLARE
		 @continue          BIT           = 1
		,@msg               VARCHAR(500)  = ''
		,@controlValue      NVARCHAR(100) = ''
		,@sqlScripts        NVARCHAR(MAX) = N''
		,@continue_W        BIT           = 1
		,@timeFrequency     VARCHAR(10)   = ''
		,@checkEverySeconds INT           = 0;
	
	SET @msg = 'Wait for me it began: ' + CONVERT(VARCHAR(20),GETDATE(),106) + ' ' + CONVERT(VARCHAR(20),GETDATE(),108);
	RAISERROR(@msg,10,1)
	
	IF(LEN(ISNULL(@serverName,'')) = 0)
		BEGIN
			SET @continue = 0;
			SET @msg      = 'Server Name parameter is required';
		END
	ELSE IF(LEN(ISNULL(@dataBaseName,'')) = 0)
		BEGIN
			SET @continue = 0;
			SET @msg      = 'Database Name parameter is required';
		END
	ELSE IF(LEN(ISNULL(@schemaName,'')) = 0)
		BEGIN
			SET @continue = 0;
			SET @msg      = 'Schema Name parameter is required';
		END
	ELSE IF(LEN(ISNULL(@tableName,'')) = 0)
		BEGIN
			SET @continue = 0;
			SET @msg      = 'Table Name parameter is required';
		END
	ELSE IF(LEN(ISNULL(@columnName,'')) = 0)
		BEGIN
			SET @continue = 0;
			SET @msg      = 'Column Name parameter is required';
		END
	ELSE IF(LEN(ISNULL(@waitValue,'')) = 0)
		BEGIN
			SET @continue = 0;
			SET @msg      = 'Wait Value parameter is required';
		END
	ELSE IF(LEN(ISNULL(@checkEveryUnit,'')) = 0)
		BEGIN
			SET @continue = 0;
			SET @msg      = 'Check Every Unit parameter is required';
		END
	ELSE IF(@checkEveryUnit NOT IN ('hour','hh','minute','min','mi','n','second','sec','ss','s'))
		BEGIN
			SET @continue = 0;
			SET @msg      = 'The value especified on Check Every Unit parameter is incorrect. Use only (hour,minute,second)';
		END
	ELSE IF (@checkEveryTime IS NULL OR @checkEveryTime <= 0)
		BEGIN
			SET @continue = 0;
			SET @msg      = 'The value especified on Check Every Time parameter is incorrect. Use only positive numbers';
		END
	
	IF(@continue = 1)
		BEGIN
			BEGIN TRY
				IF(@checkEveryUnit IN ('hour','hh'))
					BEGIN
						SET @checkEverySeconds = ((@checkEveryTime * 60) * 60);
					END
				ELSE IF(@checkEveryUnit IN ('minute','min','mi','n'))
					BEGIN
						SET @checkEverySeconds = (@checkEveryTime * 60);
					END
				ELSE IF(@checkEveryUnit IN ('second','sec','ss','s'))
					BEGIN
						SET @checkEverySeconds = @checkEveryTime;
					END
				
				SET @timeFrequency = RIGHT('0' + CAST(@checkEverySeconds / 3600 AS VARCHAR),2) + ':' + RIGHT('0' + CAST((@checkEverySeconds / 60) % 60 AS VARCHAR),2) + ':' + RIGHT('0' + CAST(@checkEverySeconds % 60 AS VARCHAR),2);
	
				SET @controlValue = @waitValue;
				
				WHILE (@waitValue = @controlValue)
					BEGIN
				
						SET @sqlScripts = 'SELECT ';
						
						IF(LEN(@whereClause) = 0)
							BEGIN
								SET @sqlScripts = @sqlScripts + ' TOP 1 ';
							END
						
						SET @sqlScripts = @sqlScripts + ' @controlValueInt = CONVERT(NVARCHAR(100),' + @columnName + ') FROM ' + @serverName + '.' + @dataBaseName + '.' + @schemaName + '.' + @tableName;
							
						IF(LEN(@whereClause) > 0)
							BEGIN
								SET @sqlScripts = @sqlScripts + ' WHERE ' + @whereClause;
							END
						
						EXEC sp_executesql @sqlScripts,N'@controlValueInt NVARCHAR(100) OUTPUT',@controlValueInt = @controlValue OUTPUT;
						
						IF(@waitValue = @controlValue)
							BEGIN
								WAITFOR DELAY @timeFrequency;
							END
					END
			END TRY
			BEGIN CATCH
				SET @continue = 0;
				SET @msg      = ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),N'') + N' - '+ ISNULL(ERROR_MESSAGE(),N'');;
			END CATCH
		END 
		
	IF(@continue = 1)
		BEGIN
			SET @msg = 'Wait for me success: ' + CONVERT(VARCHAR(20),GETDATE(),106) + ' ' + CONVERT(VARCHAR(20),GETDATE(),108);
			RAISERROR(@msg,10,1)
		END
	ELSE 
		BEGIN
			RAISERROR(@msg,11,1)
		END
	
	SET @result = @continue;
END
