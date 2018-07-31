CREATE PROCEDURE [dbo].[sp_recompileObjects] 
(
	 @schema     NVARCHAR(128) = N''
	,@objectName NVARCHAR(128) = N''
	,@objectType CHAR(2)       = ''
)
AS
BEGIN
	DECLARE
		 @continue SMALLINT
		,@errorMessage VARCHAR(500);
	
	SET @continue = 1;
	SET @errorMessage = '';
	
	IF(LEN(@schema) = 0  AND LEN(@objectName) > 0)
		BEGIN
			SET @continue = 0;
			SET @errorMessage = 'ERROR: When specifying an object name the Schema is required.';
		END
	ELSE IF(LEN(@objectType) > 0 AND (@objectType NOT IN ('P','V','FN')))
		BEGIN
			SET @continue = 0;
			SET @errorMessage = 'ERROR: The specified object type is incorrect. Use only (P for Procedures), (V for Views) or (FN for Functions).';
		END
		
	IF(@continue = 1)
		BEGIN
			IF OBJECT_ID('tempdb..#refreshObjects_result') IS NOT NULL
				DROP TABLE #refreshObjects_result;
			
			CREATE TABLE 
				#refreshObjects_result
			(
				 executionDate    DATETIME
				,objectName       NVARCHAR(256)
				,objectType       CHAR(2)
				,executionScript  NVARCHAR(300)
				,executionResult  VARCHAR(50) 
				,executionMessage VARCHAR(MAX)	
			);
		
			DECLARE 
				 @pname      NVARCHAR(256)
				,@ptype      CHAR(2)
				,@execScript NVARCHAR(300);
			
			IF (SELECT CURSOR_STATUS('global','refreshObjs_CURSOR')) >= -1
				DEALLOCATE refreshObjs_CURSOR;
			
			
			IF(LEN(@schema) > 0  AND LEN(@objectName) > 0)
				BEGIN
					DECLARE refreshObjs_CURSOR CURSOR FOR
						SELECT 
					    	 OBJECT_SCHEMA_NAME(object_id) + N'.' + name AS name
					    	,type
						FROM sys.objects
						WHERE 
							    name                          = RTRIM(LTRIM(@objectName))
							AND OBJECT_SCHEMA_NAME(object_id) = RTRIM(LTRIM(@schema));
				END
			ELSE IF(LEN(@schema) > 0 AND LEN(@objectType) > 0)
				BEGIN
					DECLARE refreshObjs_CURSOR CURSOR FOR
						SELECT 
					    	 OBJECT_SCHEMA_NAME(object_id) + N'.' + name AS name
					    	,type
						FROM sys.objects
						WHERE 
							    type                          = RTRIM(LTRIM(@objectType))
							AND OBJECT_SCHEMA_NAME(object_id) = RTRIM(LTRIM(@schema));
				END
			ELSE IF(LEN(@schema) > 0)
				BEGIN
					DECLARE refreshObjs_CURSOR CURSOR FOR
						SELECT 
					    	 OBJECT_SCHEMA_NAME(object_id) + N'.' + name AS name
					    	,type
						FROM sys.objects
						WHERE 
							OBJECT_SCHEMA_NAME(object_id) = RTRIM(LTRIM(@schema));
				END
			ELSE IF(LEN(@objectType) > 0)
				BEGIN
					DECLARE refreshObjs_CURSOR CURSOR FOR
						SELECT 
					    	 OBJECT_SCHEMA_NAME(object_id) + N'.' + name AS name
					    	,type
						FROM sys.objects
						WHERE 
							type = RTRIM(LTRIM(@objectType));
				END
			ELSE
				BEGIN
					DECLARE refreshObjs_CURSOR CURSOR FOR
						SELECT 
					    	 OBJECT_SCHEMA_NAME(object_id) + N'.' + name AS name
					    	,type
						FROM sys.objects
						WHERE type IN ('P', 'V', 'FN');
				END
			
			OPEN refreshObjs_CURSOR;
			
			FETCH NEXT FROM refreshObjs_CURSOR INTO @pname, @ptype;
			
			WHILE @@fetch_status = 0
				BEGIN
					BEGIN TRY
						SET @execScript = '';
						
						IF(@ptype = 'v')
							BEGIN
								EXEC sp_refreshview @pname;
								SET @execScript = N'EXEC sp_refreshview ' + @pname + N';';
							END
						ELSE 
							BEGIN
								EXEC sp_recompile @pname;
								SET @execScript = N'EXEC sp_recompile ' + @pname + N';';
							END
						
						INSERT INTO #refreshObjects_result
							(
								 executionDate 
								,objectName 
								,objectType 
								,executionScript 
								,executionResult
								,executionMessage
							)
						VALUES
							(
								 GETDATE()
								,@pname
								,@ptype
								,@execScript
								,'Success'
								,''
							);
					END TRY
					BEGIN CATCH
						INSERT INTO #refreshObjects_result
							(
								 executionDate 
								,objectName 
								,objectType 
								,executionScript 
								,executionResult
								,executionMessage
							)
						VALUES
							(
								 GETDATE()
								,@pname
								,@ptype
								,@execScript
								,'Error'
								,ISNULL(CONVERT(VARCHAR(50),ERROR_NUMBER()),'') + ISNULL(ERROR_MESSAGE(),'')
							);
					END CATCH
				
				    FETCH NEXT FROM refreshObjs_CURSOR INTO @pname, @ptype;
				END
				
			CLOSE refreshObjs_CURSOR;
			
			IF (SELECT CURSOR_STATUS('global','refreshObjs_CURSOR')) >= -1
				DEALLOCATE refreshObjs_CURSOR;
			
			SELECT * FROM #refreshObjects_result;
			
			IF OBJECT_ID('tempdb..#refreshObjects_result') IS NOT NULL
				DROP TABLE #refreshObjects_result;
		END
	ELSE
		BEGIN
			SELECT @errorMessage AS executionMessage;
		END
END
GO
