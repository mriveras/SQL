CREATE PROCEDURE dbo.sp_changeColumnNames 
	(
		 @sourceObjectName        NVARCHAR(128)
		,@destinationObjectName   NVARCHAR(128)
		,@charactersToFind        NVARCHAR(128)
		,@charactersToReplace     NVARCHAR(128) = ''
		,@createDestinationBackup SMALLINT      = 0
	)
AS
BEGIN
	DECLARE
		 @continue              SMALLINT
		,@message               NVARCHAR(300)
		,@sourceObjectId        INT
		,@destinationObjectId   INT
		,@sqlScripts            NVARCHAR(MAX)
		,@destinationBackupName NVARCHAR(128)
		,@tempLoadTable         NVARCHAR(128);
	
	SET @continue              = 1;
	SET @destinationBackupName = '_backup' + CONVERT(NVARCHAR(8),GETDATE(),112);
	SET @tempLoadTable         = '_TEMPCCN';
	
	IF(CHARINDEX('CHAR(',@charactersToFind) > 0)
		BEGIN
			BEGIN TRY
				DECLARE @charactersToFind2 NVARCHAR(128);
				
				SET @sqlScripts = N'SELECT @charResult = ' + @charactersToFind;
				EXECUTE sp_executesql @sqlScripts,N'@charResult NVARCHAR(128) OUTPUT',@charResult = @charactersToFind2 OUTPUT;
				
				SET @charactersToFind = @charactersToFind2;
			END TRY
			BEGIN CATCH
				--Nothing Happens, No CHAR assignation found
			END CATCH
		END
	
	IF(@charactersToReplace IS NULL)
		SET @charactersToReplace = '';
	
	IF(@createDestinationBackup IS NULL)
		SET @createDestinationBackup = 0;
	
	--CHECKING INPUT PARAMETERS
		IF(@sourceObjectName IS NULL OR DATALENGTH(RTRIM(LTRIM(@sourceObjectName))) = 0)
			BEGIN 
				SET @continue = 0;
				SET @message = 'The input parameter @sourceObjectName can not be empty'
				RAISERROR(@message,11,1);
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM sys.objects a
				WHERE
					a.object_id = OBJECT_ID(@sourceObjectName)
					AND a.type IN ('U','V')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message = 'The Source Table (' + @sourceObjectName + ') does not exists or is not a valid Table or View'
				RAISERROR(@message,11,1);
			END
		ELSE IF(@destinationObjectName IS NULL OR DATALENGTH(RTRIM(LTRIM(@destinationObjectName))) = 0)
			BEGIN 
				SET @continue = 0;
				SET @message = 'The input parameter @destinationTable can not be empty'
				RAISERROR(@message,11,1);
			END
		ELSE IF(
			NOT EXISTS(
				SELECT 1
				FROM sys.objects a
				WHERE
					a.object_id = OBJECT_ID(@destinationObjectName)
					AND a.type IN ('U')
			)
		)
			BEGIN
				SET @continue = 0;
				SET @message = 'The Destination Table (' + @destinationObjectName + ') does not exists or is not a valid Table'
				RAISERROR(@message,11,1);
			END
		ELSE IF(@charactersToFind IS NULL OR DATALENGTH(@charactersToFind) = 0)
			BEGIN
				SET @continue = 0;
				SET @message = 'The input parameter @charactersToFind can not be empty'
				RAISERROR(@message,11,1);
			END
		ELSE IF(@createDestinationBackup < 0 OR @createDestinationBackup > 1)
			BEGIN
				SET @continue = 0;
				SET @message = 'The input parameter @createDestinationBackup only accept (1) for true or (0) for false'
				RAISERROR(@message,11,1);
			END

	IF(@continue = 1)
		BEGIN
			--FINDING OBJECT IDs
				SET @sourceObjectId      = OBJECT_ID(@sourceObjectName);
				SET @destinationObjectId = OBJECT_ID(@destinationObjectName);
	
			BEGIN TRANSACTION
			
			BEGIN TRY
				--CHEKING IF SOURCE TABLE HAS THE CHARACTERS TO FIND IN AT LEAST ONE COLUMN
					IF(
						EXISTS(
							SELECT 1
							FROM sys.columns a
							WHERE
								    a.object_id = @sourceObjectId
								AND a.name LIKE N'%' + @charactersToFind + N'%'
						)
					)
						BEGIN
							--BACKUP DESTINATION
								IF(@destinationObjectId IS NOT NULL AND @createDestinationBackup = 1)
									BEGIN
										SET @sqlScripts = N'SELECT * INTO ' + @destinationObjectName + @destinationBackupName + N' FROM ' + @destinationObjectName;
										EXEC(@sqlScripts);
										
										IF(@sourceObjectId <> @destinationObjectId)
											BEGIN
												SET @sqlScripts = N'DROP TABLE ' + @destinationObjectName;
												EXEC(@sqlScripts);
											END
									END
									
							--CHANGING NAMES INTO TEMP TABLE 
								SELECT
									@sqlScripts = 'SELECT ' + 
									CONVERT(NVARCHAR(max),
										STUFF(
											(
												SELECT
													N', ' +
													CASE
														WHEN (a.name LIKE N'%' + @charactersToFind + N'%') THEN 
															N'[' + a.name + N'] AS [' + REPLACE(a.name,@charactersToFind,@charactersToReplace) + N']'
														ELSE
															N'[' + a.name + N']'
													END
												FROM
													sys.columns a
												WHERE
													a.object_id = @sourceObjectId
												ORDER BY
													a.column_id ASC 
												FOR XML PATH(''), TYPE
											).value('.', 'VARCHAR(MAX)'), 1, 2, ''
										)
									);
								IF(@sourceObjectId = @destinationObjectId)
									BEGIN
										SET @sqlScripts = @sqlScripts + N' INTO ' + @destinationObjectName + @tempLoadTable
									END
								ELSE
									BEGIN
										SET @sqlScripts = @sqlScripts + N' INTO ' + @destinationObjectName
									END
								SET @sqlScripts = @sqlScripts + ' FROM ' + @sourceObjectName;
								EXEC(@sqlScripts);
								
							IF(@sourceObjectId = @destinationObjectId)
								BEGIN
									--COPYING TEMP TABLE INTO DESTINATION TABLE
										SET @sqlScripts = 'DROP TABLE ' + @destinationObjectName;
										EXEC(@sqlScripts);
										
										SET @sqlScripts = 'SELECT * INTO ' + @destinationObjectName + ' FROM ' + @destinationObjectName + @tempLoadTable;
										EXEC(@sqlScripts);
										
										SET @sqlScripts = 'DROP TABLE ' + @destinationObjectName + @tempLoadTable;
										EXEC(@sqlScripts);
								END
						END
				
				COMMIT TRANSACTION;
			END TRY
			BEGIN CATCH
				SET @message = ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),N'') + N' - '+ ISNULL(ERROR_MESSAGE(),N'');
				RAISERROR(@message,11,1);
				ROLLBACK TRANSACTION;
			END CATCH
		END
END
GO
