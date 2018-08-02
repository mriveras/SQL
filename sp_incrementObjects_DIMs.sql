CREATE PROCEDURE [dbo].[sp_incrementObjects_DIMs] 
	(
		 @sourceSchema     NVARCHAR(128)
		,@sourceObjectName NVARCHAR(128)
		,@dimHashSchema    NVARCHAR(128)
		,@dimHashTableName NVARCHAR(128)
		,@debug            SMALLINT      = 0
		,@loggingType      SMALLINT      = 1 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
	)
AS
/*
	Developed by: Mauricio Rivera
	Date: 17 Apr 2018
	
	MODIFICATIONS:
		
	LAST USED LOGGING IDS:
		- ERRORS      (COD-3200E)
		- INFORMATION (COD-1900I)
*/
BEGIN
	--Transforming input parameter from NULL to default value
		IF(@debug IS NULL)
			SET @debug = 0;
			
		IF(@loggingType IS NULL)
			SET @loggingType = 1;
	
	--checking sequence
		IF(
			NOT EXISTS(
				SELECT 1
				FROM sys.sequences 
				WHERE name = 'sq_BI_log_executionID'
			)
		)
			BEGIN
				CREATE SEQUENCE dbo.sq_BI_log_executionID
			    	START     WITH 1  
			    	INCREMENT BY   1; 
			END
			
	IF(OBJECT_ID('dbo.BI_log') IS NULL)
		BEGIN
			CREATE TABLE dbo.BI_log
			(
				executionID BIGINT         NOT NULL,
				sequenceID  INT            NOT NULL,
				logDateTime DATETIME       NOT NULL,
				object      VARCHAR (256)  NOT NULL,
				scriptCode  VARCHAR (25)   NOT NULL,
				status      VARCHAR (50)   NOT NULL,
				message     VARCHAR (500)  NOT NULL,
				SQL         VARCHAR (4000) NOT NULL,
				variables   VARCHAR (2500) NOT NULL,
				CONSTRAINT PK_BI_log PRIMARY KEY (executionID, sequenceID)
			);
		END
	
	--Declaring User Table for Log purpose
		DECLARE @BI_log TABLE (			
			 executionID BIGINT
			,sequenceID  INT IDENTITY(1,1)
			,logDateTime DATETIME
			,object      VARCHAR (256)
			,scriptCode  VARCHAR (25)
			,status      VARCHAR (50)
			,message     VARCHAR (500)
			,SQL         VARCHAR (4000)
			,variables   VARCHAR (2500)
		);
	
	DECLARE
	--PROCESS FLOW VARIABLES
		 @continue                  BIT            = 1
		,@sqlScript                 NVARCHAR(MAX)  = N''
		,@INT                       INT            = 0
	--LOGGING VARIABLES
		,@executionID               BIGINT         = NEXT VALUE FOR dbo.sq_BI_log_executionID
		,@execObjectName            VARCHAR(256)   = 'dbo.sp_incrementObjects_DIMs'
		,@scriptCode                VARCHAR(25)    = ''
		,@status                    VARCHAR(50)    = ''
		,@logTreeLevel              TINYINT        = 0
		,@logSpaceTree              VARCHAR(5)     = '    '
		,@message                   VARCHAR(500)   = ''
		,@SQL                       VARCHAR(4000)  = ''
		,@variables                 VARCHAR(2500)  = ''
	--FLAGS VARIABLES
		,@changesFound              BIT            = 0
		,@DIM                       BIT            = 0
		,@VIEW                      BIT            = 0
		,@firstLoad                 BIT            = 0
		,@reloadProcess             BIT            = 0
	--GENERAL VARIABLES
		,@tempHashV1ObjectName      NVARCHAR(128)  = N''
		,@sourceFullObject          NVARCHAR(256)  = N''
		,@dimHashFullObject         NVARCHAR(256)  = N''
		,@tempHashV1FullObject      NVARCHAR(156)  = N''
		,@sourceObjectId            INT            = 0
		,@dimHashObjectId           INT            = 0
		,@columns                   NVARCHAR(4000) = N''
		,@asAtDateProcessed         DATETIME       = GETDATE()
		,@asAtDateProcessed_varchar VARCHAR(50)    = N'';
		
	SET @sourceFullObject     = @sourceSchema  + N'.' + @sourceObjectName;
	SET @sourceObjectId       = OBJECT_ID(@sourceFullObject);
	SET @tempHashV1ObjectName = @sourceObjectName + '_STG';
	SET @tempHashV1FullObject = @sourceSchema + N'.' + @tempHashV1ObjectName;
	SET @dimHashFullObject    = @dimHashSchema + N'.' + @dimHashTableName;
	SET @dimHashObjectId      = OBJECT_ID(@dimHashFullObject);
	
	/*-----------------------------------------------------------------------------------------------------------------------------------------------
	 ***********************************************************************************************************************************************
	   IF IS A RELOAD PROCESS, CHANGE THE VALUE OF THE COLUMNS VALUE2 IN THE CONFIG TABLE dbo.BIConfig AND SET TO (1) THE VALUE OF THE COLUMN VALUE1
	   
	   USE THE FOLLOWING SELECT TO GET THE VALUE OF THE REPROCESS PROCESS IN THE CONFIG TABLE
	   		- SELECT value1, value2 FROM dbo.BIConfig WHERE type = 'REPROCESS-DATE-DIM';
	   
	   USE THE FOLLOWING SCRIPT TO UPDATE THE COLUMN VALUE1 IN THE CONFIG TABLE (1 = Reprocess Activated | 0 = Reprocess No Activated)	
	   		- UPDATE INTO dbo.BIConfig SET value1 = '0' WHERE type = 'REPROCESS-DATE-DIM';
	   		
	   USE THE FOLOWING SCRIPT TO UPDATE THE COLUMN VALUE2 IN THE CONFIG TABLE (As At Date to be reprocessed)
	   		- UPDATE INTO dbo.BIConfig SET value2 = '31 Dec 9999 11:59:59 PM' WHERE type = 'REPROCESS-DATE-DIM';
	   		- THE FORMAT FOR THE VALUE OF THIS COLUMNS VALUE1 IS EG '31 Dec 9999 11:59:59 PM'
	 ***********************************************************************************************************************************************
	-------------------------------------------------------------------------------------------------------------------------------------------------*/
		SET @reloadProcess = (SELECT dbo.udf_getBIConfigParameter('REPROCESS-DATE-DIM',1));
	/*-----------------------------------------------------------------------------------------------------------------------------------------------
	 ***********************************************************************************************************************************************
	 ***********************************************************************************************************************************************
	-------------------------------------------------------------------------------------------------------------------------------------------------*/
	
	SET @variables = ' | @sourceSchema = '         + ISNULL(CONVERT(VARCHAR(128),@sourceSchema)        ,'') +
					 ' | @sourceObjectName = '     + ISNULL(CONVERT(VARCHAR(128),@sourceObjectName)    ,'') +
					 ' | @dimHashSchema = '        + ISNULL(CONVERT(VARCHAR(128),@dimHashSchema)       ,'') +
					 ' | @dimHashTableName = '     + ISNULL(CONVERT(VARCHAR(128),@dimHashTableName)    ,'') +
					 ' | @sourceFullObject = '     + ISNULL(CONVERT(VARCHAR(128),@sourceFullObject)    ,'') +
					 ' | @tempHashV1FullObject = ' + ISNULL(CONVERT(VARCHAR(128),@tempHashV1FullObject),'') +
					 ' | @dimHashFullObject = '    + ISNULL(CONVERT(VARCHAR(128),@dimHashFullObject)   ,'') +
					 ' | @debug = '                + ISNULL(CONVERT(VARCHAR(1),@debug)                 ,'') +
					 ' | @loggingType = '          + ISNULL(CONVERT(VARCHAR(1),@loggingType)           ,'');
	
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		SET @logTreeLevel = 0;
		SET @scriptCode   = '';
		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Store Procedure';
		SET @status       = 'Information';
		SET @SQL          = '';
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO dbo.BI_log (executionID,sequenceID,logDateTime,object,scriptCode,status,message,SQL,variables)
				VALUES (@executionID,0,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
			END
		IF(@loggingType IN (2,3))
			RAISERROR(@message,10,1);
		
		SET @execObjectName = '';--This variable is set to BLANK because it's not necessary to set the same value in all the log records	
		SET @variables      = '';--This variable is set to BLANK because it's not necessary to set the same value in all the log records	
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
	
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 1;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Transaction';
				SET @status       = 'Information';
				SET @SQL          = '';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@message,10,1);
			END
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			
	--CREATING THE ROLLBACK FLAG
		BEGIN TRANSACTION;
		
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 2;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Input Parameter Validation';
				SET @status       = 'Information';
				SET @SQL          = '';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@message,10,1);
			END
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
	
	--If the variables is (1) check the existence of the input value at the config table
		IF(@reloadProcess = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Reprocess Process activated';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
							   	RAISERROR(@message,10,1);
						END 
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Getting asAtDateProcessed parameter from Config Table';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
							   	RAISERROR(@message,10,1);
						END 
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					SET @asAtDateProcessed = (SELECT CONVERT(DATETIME,dbo.udf_getBIConfigParameter('REPROCESS-DATE-DIM',2)));
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'asAtDateProcessed assigned to (' + CONVERT(VARCHAR(50),@asAtDateProcessed,100) + ')';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
								   	RAISERROR(@message,10,1);
							END 
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					IF(LEN(RTRIM(LTRIM(ISNULL(@asAtDateProcessed,'')))) = 0)
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-100E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
								SET @status       = 'ERROR';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
								   	RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Getting asAtDateProcessed parameter from Config Table';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
							   	RAISERROR(@message,10,1);
						END 
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
	
	--CONVERTING BI_beginDate into VARCHAR
		IF(@continue = 1)
			BEGIN
				SET @asAtDateProcessed_varchar = CONVERT(VARCHAR(50),@asAtDateProcessed,100);
			END 
			
	IF(@sourceSchema IS NULL OR LEN(RTRIM(LTRIM(@sourceSchema))) = 0)
		BEGIN			
			SET @continue = 0;
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				SET @logTreeLevel = 3;
				SET @scriptCode   = 'COD-200E';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Parameter @sourceSchema can not be empty or NULL';
				SET @status       = 'ERROR';
				SET @SQL          = '';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@message,11,1);
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		END
	ELSE IF(@sourceObjectName IS NULL OR LEN(RTRIM(LTRIM(@sourceObjectName))) = 0)
		BEGIN			
			SET @continue = 0;
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				SET @logTreeLevel = 3;
				SET @scriptCode   = 'COD-300E';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Parameter @sourceObjectName can not be empty or NULL';
				SET @status       = 'ERROR';
				SET @SQL          = '';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@message,11,1);
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		END
	ELSE IF(@dimHashSchema IS NULL OR LEN(RTRIM(LTRIM(@dimHashSchema))) = 0)
		BEGIN
			SET @continue = 0;
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				SET @logTreeLevel = 3;
				SET @scriptCode   = 'COD-400E';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Parameter @dimHashSchema can not be empty or NULL';
				SET @status       = 'ERROR';
				SET @SQL          = '';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@message,11,1);
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		END
	ELSE IF(@dimHashTableName IS NULL OR LEN(RTRIM(LTRIM(@dimHashTableName))) = 0)
		BEGIN
			SET @continue = 0;
			----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
				SET @logTreeLevel = 3;
				SET @scriptCode   = 'COD-500E';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Parameter @dimHashTableName can not be empty or NULL';
				SET @status       = 'ERROR';
				SET @SQL          = '';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@message,11,1);
			----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		END
	
	--VALIDATE SOURCE OBJECT EXISTS
		IF(
			@continue = 1 
			AND (
				@sourceObjectId IS NULL
				OR EXISTS(
					SELECT 1
					FROM sys.objects a
					WHERE
						a.object_id = @sourceObjectId
						AND a.type NOT IN ('U','V')
				)
			)
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-600E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The object ' + @sourceFullObject + ' does not exists or is not a valid Table or VIEW';
					SET @status       = 'ERROR';
					SET @SQL          = '';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
							VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@message,11,1);
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
	
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		IF(@debug = 1 AND @continue = 1)
			BEGIN
				SET @logTreeLevel = 3;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Input Parameter Validation Sucessful';
				SET @status       = 'Information';
				SET @SQL          = '';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@message,10,1);
			END
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
	
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 2;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Input Parameter Validation';
				SET @status       = 'Information';
				SET @SQL          = '';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@message,10,1);
			END
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
	
	--VALIDATING THE DATA ON THE SOURCE
		IF(@continue = 1)
			BEGIN 
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Data Validation at the Source';
							SET @status       = 'Information';
							SET @SQL          = ''; 
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				BEGIN TRY
					SET @VIEW = 0;
					SET @sqlScript = N'SELECT DISTINCT @exist = 1 FROM ' + @sourceFullObject;
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-100I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScript, N'@exist SMALLINT OUTPUT', @exist = @VIEW OUTPUT;
					
					IF(@VIEW = 0) --If no error happens, the table exists. However, if @VIEW is 0, the table is empty. That's why an error is triggered
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-700E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Source Object (' + @sourceFullObject + ') is emptyt';
								SET @status       = 'ERROR';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
					ELSE
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)--MAURICIO 2
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Source Object (' + @sourceFullObject + ') has data on it';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-800E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END BEGIN Data Validation at the Source';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
	
	--VALIDATING THE DIM TABLE
		IF(@continue = 1)
			BEGIN 
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN DIM Table Validation';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				
				BEGIN TRY
					SET @DIM = 0;
					SET @sqlScript = N'SELECT DISTINCT @exist = 1 FROM ' + @dimHashFullObject;
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-200I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScript, N'@exist SMALLINT OUTPUT', @exist = @DIM OUTPUT;
					
					IF(@DIM = 0) --If no error happens, the table exists. However, if @DIM is 0, the table is empty. That's why we change it to 1
						BEGIN
							SET @DIM = 1;
						END
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'DIM table (' + @dimHashFullObject + ') exists';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END TRY
				BEGIN CATCH
					SET @DIM = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'DIM table (' + @dimHashFullObject + ') does not exists';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END DIM Table Validation';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
	
	--GENERATE FIRST VERSION OF THE HASH AT THE SOURCE LEVEL
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Generating Hash V1 Into Table (' + @tempHashV1FullObject + ')';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				
				BEGIN TRY
					IF(OBJECT_ID(@tempHashV1FullObject) IS NOT NULL)
						BEGIN 
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Table (' + @tempHashV1FullObject + ') found. Proceeding to drop it.';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
							SET @sqlScript = N'DROP TABLE ' + @tempHashV1FullObject;
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-300I';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
										SET @status       = 'Information';
										SET @SQL          = ISNULL(@sqlScript,'');
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
							EXEC(@sqlScript);
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Table (' + @tempHashV1FullObject + ') dropped successfully';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-900E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				
				BEGIN TRY
					SET @sqlScript = N'EXEC dbo.sp_generateHashKey ''' + @sourceSchema + ''',''' + @sourceObjectName + ''',''' + @sourceSchema + ''',''' + @tempHashV1ObjectName + ''','''','''','''',0,3';
								
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-400I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC(@sqlScript);

					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Hash Key V1 created successfully';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1000E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Generating Hash V1 Into the Table (' + @tempHashV1FullObject + ')';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
	
	--BEGIN RENAMING THE COLUMN BI_FHR WHICH BEING USED FOR HASH KEY V1
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Renaming Columns BI_HFR used for the Hash Key V1';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				BEGIN TRY
					SET @sqlScript = N'EXEC sp_RENAME ''' + @tempHashV1FullObject + '.BI_HFR'',''BI_HFR_V1'',''COLUMN''';				
			
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-500I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC(@sqlScript);
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Column renamed successfully';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END TRY
				BEGIN CATCH
					SET @continue   = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1100E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Renaming Columns BI_HFR used for the Hash Key V1';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
	
	--BEGIN CREATING THE DUMMY COLUMN BI_FHR. Because the Homogenizing Table Structure requiered it.
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Creating Column BI_HFR in (' + @tempHashV1FullObject + ')';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				BEGIN TRY
					SET @sqlScript = N'ALTER TABLE ' + @tempHashV1FullObject + ' ADD BI_HFR VARCHAR(40) NULL';				
			
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-600I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC(@sqlScript);
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Column BI_HFR created successfully';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END TRY
				BEGIN CATCH
					SET @continue   = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1200E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Creating Column BI_HFR in (' + @tempHashV1FullObject + ')';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END

	--IF DIM TABLE DOES NOT EXIST, LETS CREATE IT
		IF(@continue = 1 AND @DIM = 0)
			BEGIN
				SET @firstLoad = 1;
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Creating DIM Table (' + @dimHashFullObject + ')';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				BEGIN TRY
					SET @sqlScript = N'SELECT *, CONVERT(DATETIME,''' + @asAtDateProcessed_varchar + ''') AS BI_beginDate, CONVERT(DATETIME,''31 Dec 9999 11:59:59 PM'') AS BI_endDate INTO ' + @dimHashFullObject + N' FROM ' + @tempHashV1FullObject;
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-700I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC(@sqlScript);
					SET @DIM = 1;
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking data';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					--Data Validation
						SET @sqlScript = N'SELECT DISTINCT @count = COUNT(*) FROM ' + @dimHashFullObject;				
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 4;
									SET @scriptCode   = 'COD-800I';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
									SET @status       = 'Information';
									SET @SQL          = ISNULL(@sqlScript,'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,10,1);
								END
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						
						EXEC sp_executesql @sqlScript, N'@count INT OUTPUT', @count = @INT OUTPUT;
						
						IF(@INT IS NULL OR @INT = 0)
							BEGIN
								SET @continue = 0;

								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 4;
									SET @scriptCode   = 'COD-1300E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The DIM table does not have records after its creation';
									SET @status       = 'ERROR';
									SET @SQL          = ''
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,11,1);
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							END
							
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1 AND @continue = 1)
								BEGIN
									SET @logTreeLevel = 4;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Data Checked Sucessfully';
									SET @status       = 'Information';
									SET @SQL          = '';
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,10,1);
								END
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Data';
									SET @status       = 'Information';
									SET @SQL          = '';
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,10,1);
								END
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END TRY
				BEGIN CATCH
					SET @continue   = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1400E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
			
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Creating DIM Table';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------

				--BEGIN GENERATING THE HASH KEY V2
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Generating Hash V2 Into (' + @dimHashFullObject + ')';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					BEGIN TRY
						SET @columns = (
							SELECT
								STUFF(
									(
										SELECT   
											N',' + a.name
										FROM     
											sys.columns a 
										WHERE
											    a.object_id = OBJECT_ID(@dimHashFullObject)
											AND a.name NOT IN ('BI_endDate','BI_HFR')
										ORDER BY 
											a.name ASC
										FOR XML  PATH(''), TYPE
									).value('.', 'VARCHAR(MAX)'), 1, 1, ''
								)
						);
						
						SET @sqlScript = N'EXEC dbo.sp_generateHashKey ''' + @dimHashSchema + ''',''' + @dimHashTableName + ''',''' + @dimHashSchema + ''',''' + @dimHashTableName + ''',''' + @columns + ''','''','''',1,3';
									
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-900I';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
									SET @status       = 'Information';
									SET @SQL          = ISNULL(@sqlScript,'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,10,1);
								END
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						
						EXEC(@sqlScript);
	
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Hash Key V2 created successfully';
									SET @status       = 'Information';
									SET @SQL          = '';
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,10,1);
								END
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					END TRY
					BEGIN CATCH
						SET @continue = 0;
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							SET @logTreeLevel = 3;
							SET @scriptCode   = 'COD-1500E';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
							SET @status       = 'ERROR';
							SET @SQL          = ISNULL(@sqlScript,'');
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,11,1);
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					END CATCH
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Generating Hash V2 Into (' + @dimHashFullObject + ')';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
	
	--CHECKING INDEX OVER @tempHashV1FullObject BI_HFR_V1 column
		IF(@continue = 1 AND @firstLoad = 0)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Index on (' + @tempHashV1FullObject + ') over the column BI_HFR_V1';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				
				BEGIN TRY
					SET @sqlScript = N'EXEC dbo.sp_manageIndexes 1, 2, ''IL_NC_' + @sourceSchema + @tempHashV1ObjectName + N'_BI_HFR_V1'',''' + @sourceSchema + ''',''' + @tempHashV1ObjectName + ''',''BI_HFR_V1'','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1000I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScript, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1600E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
								SET @status       = 'ERROR';
								SET @SQL          = @SQL;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1700E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------g
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Index on (' + @tempHashV1FullObject + ') over the column BI_HFR_V1';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
	
	--CHECKING INDEX OVER @dimHashFullObject BI_HFR_V1 column
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Index on (' + @dimHashFullObject + ') over the column BI_HFR_V1';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				
				BEGIN TRY
					SET @sqlScript = N'EXEC dbo.sp_manageIndexes 1, 2, ''IL_NC_' + @dimHashSchema + @dimHashTableName + N'_BI_HFR_V1'',''' + @dimHashSchema + ''',''' + @dimHashTableName + ''',''BI_HFR_V1'','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1100I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScript, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)--MAURICIO 1
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1800E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
								SET @status       = 'ERROR';
								SET @SQL          = @SQL;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1900E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------g
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Index on (' + @dimHashFullObject + ') over the column BI_HFR_V1';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
			
	--CHECKING INDEX OVER @dimHashFullObject BI_beginDate column
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Index on (' + @dimHashFullObject + ') over the column BI_beginDate';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				
				BEGIN TRY
					SET @sqlScript = N'EXEC dbo.sp_manageIndexes 1, 2, ''IL_NC_' + @dimHashSchema + @dimHashTableName + N'_BI_beginDate'',''' + @dimHashSchema + ''',''' + @dimHashTableName + ''',''BI_beginDate'','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1200I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScript, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1) --MAURICIO 3
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-2000E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
								SET @status       = 'ERROR';
								SET @SQL          = @SQL;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-2100E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------g
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Index on (' + @dimHashFullObject + ') over the column BI_beginDate';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
	
	--CHECKING INDEX OVER @dimHashFullObject BI_endDate column
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Index on (' + @dimHashFullObject + ') over the column BI_endDate';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				
				BEGIN TRY
					SET @sqlScript = N'EXEC dbo.sp_manageIndexes 1, 2, ''IL_NC_' + @dimHashSchema + @dimHashTableName + N'_BI_endDate'',''' + @dimHashSchema + ''',''' + @dimHashTableName + ''',''BI_endDate'','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1300I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScript, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)--MAURICIO 4
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-2200E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
								SET @status       = 'ERROR';
								SET @SQL          = @SQL;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-2300E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------g
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Index on (' + @dimHashFullObject + ') over the column BI_endDate';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END

	--HOMOGENIZING DIM TABLE STRUCTURE WITH THE TABLE @tempHashV1FullObject
		IF(@continue = 1 AND @firstLoad = 0)
			BEGIN 			
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Homogenizing DIM structure with the table (' + @tempHashV1FullObject + ')';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				
				BEGIN TRY
					SET @sqlScript = N'EXEC dbo.sp_homogeniseObjectStructure @objectFrom = ''' + @tempHashV1FullObject + ''', @objectTo = ''' + @dimHashFullObject + ''', @addNewColumns = 1, @dropNonUsedColumns = 0, @alterDataType = 1, @dontLoseDataWhenDataTypeChange = 1, @status = @statusInt OUTPUT, @message = @messageInt OUTPUT, @SQL = @SQLInt OUTPUT';

					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1400I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = ISNULL(@sqlScript,'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScript, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(4000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
										
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)--MAURICIO 5
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
					ELSE
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-2400E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + @message;
								SET @status       = 'ERROR';
								SET @SQL          = @SQL;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,11,1);
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				END TRY
				BEGIN CATCH
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-2500E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						SET @status       = 'ERROR';
						SET @SQL          = ISNULL(@sqlScript,'');
						IF(@loggingType IN (1,3))
							BEGIN
								INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
								VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
							END
						IF(@loggingType IN (2,3))
							RAISERROR(@message,11,1);
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Homogenizing DIM structure';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END

	--INCREMENTAL PROCESS
		IF(@continue = 1 AND @DIM = 1 AND @VIEW = 1 AND @firstLoad = 0)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Incremental Process';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				
				--GETTING CHANGED / DELETED ROWS
					IF(@continue = 1)
						BEGIN
							SET @changesFound = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Getting changed / deleted rows';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							BEGIN TRY
								IF OBJECT_ID ('tempdb..##incrementObjects_DIM_changeDelete') IS NOT NULL
									DROP TABLE ##incrementObjects_DIM_changeDelete;	
								
								SET @sqlScript =               N'SELECT ';
								SET @sqlScript = @sqlScript +     N'a.BI_HFR ';
								SET @sqlScript = @sqlScript + N'INTO ';
								SET @sqlScript = @sqlScript +     N'##incrementObjects_DIM_changeDelete ';
								SET @sqlScript = @sqlScript + N'FROM ';
								SET @sqlScript = @sqlScript +     @dimHashFullObject + N' a LEFT JOIN ' + @tempHashV1FullObject + N' b ON ';
								SET @sqlScript = @sqlScript +         N'b.BI_HFR_V1 = a.BI_HFR_V1 ';
								SET @sqlScript = @sqlScript + N'WHERE ';
								SET @sqlScript = @sqlScript +     N'b.BI_HFR_V1 IS NULL ';
								SET @sqlScript = @sqlScript +     N'AND a.BI_endDate = CONVERT(DATETIME,''31 Dec 9999 11:59:59 PM'') ';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1500I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = ISNULL(@sqlScript,'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									
								EXEC(@sqlScript);
								
								SET @INT = @@ROWCOUNT;
								
								IF(@INT > 0)
									BEGIN
										SET @changesFound = 1;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + CONVERT(NVARCHAR(20),@INT) + ' Row(s) Affected';
													SET @status       = 'Information';
													SET @SQL          = '';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END
								ELSE
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)--MAURICIO 6
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + N'No rows affected';
													SET @status       = 'Information';
													SET @SQL          = '';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END 
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								
								IF OBJECT_ID ('tempdb..##incrementObjects_DIM_changeDelete') IS NOT NULL
									DROP TABLE ##incrementObjects_DIM_changeDelete;
									
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel  = 3;
									SET @scriptCode   = 'COD-2600E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
									SET @status       = 'ERROR';
									SET @SQL          = ISNULL(@sqlScript,'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,11,1);
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							END CATCH
						END
				
				--UPDATING CHANGED / DELETED ROWS INTO DIM TABLE
					IF(@continue = 1 AND @changesFound = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Updating changed / deleted rows on DIM table';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							BEGIN TRY
								SET @sqlScript =               N'UPDATE a ';
								SET @sqlScript = @sqlScript + N'SET a.BI_endDate = CONVERT(DATETIME,''' + @asAtDateProcessed_varchar + ''')';
								SET @sqlScript = @sqlScript + N'FROM ' + @dimHashFullObject + N' a INNER JOIN ##incrementObjects_DIM_changeDelete b ON ';
								SET @sqlScript = @sqlScript + N'b.BI_HFR = a.BI_HFR ';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1600I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = ISNULL(@sqlScript,'');
											IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									
								EXEC(@sqlScript);
								
								SET @INT = @@ROWCOUNT;
								
								IF(@INT > 0)
									BEGIN 
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)--MAURICIO 7
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + CONVERT(NVARCHAR(10),@INT) + ' Row(s) Affected';
													SET @status       = 'Information';
													SET @SQL          = '';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END
								ELSE
									BEGIN
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel   = 3;
											SET @scriptCode     = N'COD-2700E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + N'No Rows Affected';
											SET @status         = N'ERROR';
											SET @SQL        = N'';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,11,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END
								
								IF OBJECT_ID ('tempdb..##incrementObjects_DIM_changeDelete') IS NOT NULL
									DROP TABLE ##incrementObjects_DIM_changeDelete;
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								
								IF OBJECT_ID ('tempdb..##incrementObjects_DIM_changeDelete') IS NOT NULL
									DROP TABLE ##incrementObjects_DIM_changeDelete;
									
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-2800E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
									SET @status       = 'ERROR';
									SET @SQL          = ISNULL(@sqlScript,'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,11,1);
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							END CATCH
						END
				
				--GETTING NEW ROWS
					IF(@continue = 1)
						BEGIN
							SET @changesFound = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Getting new rows';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							BEGIN TRY
								IF OBJECT_ID ('tempdb..##incrementObjects_DIM_new') IS NOT NULL
									DROP TABLE ##incrementObjects_DIM_new;	
								
								SET @sqlScript =               N'SELECT '
								SET @sqlScript = @sqlScript +     N'a.BI_HFR_V1 ';
								SET @sqlScript = @sqlScript + N'INTO ';
								SET @sqlScript = @sqlScript +     N'##incrementObjects_DIM_new ';
								SET @sqlScript = @sqlScript + N'FROM ';
								SET @sqlScript = @sqlScript +     @tempHashV1FullObject + N' a LEFT JOIN ' + @dimHashFullObject + N' b ON ';
								SET @sqlScript = @sqlScript +         N'b.BI_HFR_V1 = a.BI_HFR_V1 ';
								SET @sqlScript = @sqlScript +         N'AND b.BI_endDate = CONVERT(DATETIME,''31 Dec 9999 11:59:59 PM'') ';
								SET @sqlScript = @sqlScript + N'WHERE ';
								SET @sqlScript = @sqlScript +     N'b.BI_HFR_V1 IS NULL ';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1700I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = ISNULL(@sqlScript,'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									
								EXEC(@sqlScript);
								
								SET @INT = @@ROWCOUNT;
								
								IF(@INT > 0)
									BEGIN 
										SET @changesFound = 1;
										IF(@debug = 1)--MAURICIO 8
											BEGIN
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + CONVERT(NVARCHAR(10),@INT) + ' Row(s) Affected';
													SET @status       = 'Information';
													SET @SQL          = '';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											END
									END
								ELSE
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'No Rows Affected';
											SET @status       = 'Information';
											SET @SQL          = '';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								
								IF OBJECT_ID ('tempdb..##incrementObjects_DIM_new') IS NOT NULL
									DROP TABLE ##incrementObjects_DIM_new;	
									
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-2900E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
									SET @status       = 'ERROR';
									SET @SQL          = ISNULL(@sqlScript,'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,11,1);
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							END CATCH
						END
						
				--INSERTING NEW ROWS INTO DIM TABLE
					IF(@continue = 1 AND @changesFound = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Inserting new rows';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							BEGIN TRY
								--GETTING @dimHashFullObject COLUMNS
									SET @columns = (
										SELECT
											STUFF(
												(
													SELECT   
														N',[' + a.name + N']'
													FROM     
														sys.columns a INNER JOIN sys.columns b ON
															    a.name = b.name
															AND a.object_id = OBJECT_ID(@tempHashV1FullObject)
															AND b.object_id = OBJECT_ID(@dimHashFullObject)
													WHERE    
														a.name NOT IN ('BI_beginDate','BI_endDate')
													ORDER BY 
														a.name ASC
													FOR XML  PATH(''), TYPE
												).value('.', 'VARCHAR(MAX)'), 1, 1, ''
											)
											+ ',[BI_beginDate],[BI_endDate]'
									);
									
								SET @sqlScript =               N'INSERT INTO ' + @dimHashFullObject + N' (' + @columns + N') ';
								
								SET @columns = (
									SELECT
										STUFF(
											(
												SELECT   
													N',b.[' + a.name + N']'
												FROM     
													sys.columns a INNER JOIN sys.columns b ON
														    a.name = b.name
														AND a.object_id = OBJECT_ID(@tempHashV1FullObject)
														AND b.object_id = OBJECT_ID(@dimHashFullObject)
												WHERE    
													a.name NOT IN ('BI_beginDate','BI_endDate')
												ORDER BY 
													a.name ASC
												FOR XML  PATH(''), TYPE
											).value('.', 'VARCHAR(MAX)'), 1, 1, ''
										)
										+ ',CONVERT(DATETIME,''' + @asAtDateProcessed_varchar + ''') AS [BI_beginDate],CONVERT(DATETIME,''31 Dec 9999 11:59:59 PM'') as [BI_endDate]'
								);
									
								SET @sqlScript = @sqlScript +     N'SELECT ' + @columns
								SET @sqlScript = @sqlScript +     N'FROM ';
								SET @sqlScript = @sqlScript +         N'##incrementObjects_DIM_new a INNER JOIN ' + @tempHashV1FullObject + N' b ON ';
								SET @sqlScript = @sqlScript +             N'b.BI_HFR_V1 = a.BI_HFR_V1 ';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1800I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = ISNULL(@sqlScript,'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									
								EXEC(@sqlScript);
										
								SET @INT = @@ROWCOUNT;
								
								IF(@INT > 0)
									BEGIN 
										SET @changesFound = 1;
										IF(@debug = 1)
											BEGIN
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + CONVERT(NVARCHAR(10),@INT) + ' Row(s) Affected';
													SET @status       = 'Information';
													SET @SQL          = '';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											END
									END
								ELSE
									BEGIN
										SET @continue = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-3000E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'No Rows Affected';
											SET @status       = 'ERROR';
											SET @SQL          = '';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,11,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END
									
								IF OBJECT_ID ('tempdb..##incrementObjects_DIM_new') IS NOT NULL
									DROP TABLE ##incrementObjects_DIM_new;		
							END TRY
							BEGIN CATCH
								SET @continue = 0;									
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-3100E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
									SET @status       = 'ERROR';
									SET @SQL          = ISNULL(@sqlScript,'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,11,1); 
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------	
							END CATCH
						END

				--BEGIN GENERATING THE HASH KEY V2
					IF(@continue = 1 )
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Generating Hash V2 Into (' + @dimHashFullObject + ')';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							BEGIN TRY
								SET @columns = (
									SELECT
										STUFF(
											(
												SELECT   
													N',' + a.name
												FROM     
													sys.columns a 
												WHERE
													    a.object_id = OBJECT_ID(@dimHashFullObject)
													AND a.name NOT IN ('BI_endDate','BI_HFR')
												ORDER BY 
													a.name ASC
												FOR XML  PATH(''), TYPE
											).value('.', 'VARCHAR(MAX)'), 1, 1, ''
										)
								);
								
								SET @sqlScript = N'EXEC dbo.sp_generateHashKey ''' + @dimHashSchema + ''',''' + @dimHashTableName + ''',''' + @dimHashSchema + ''',''' + @dimHashTableName + ''',''' + @columns + ''','''','''',1,3';
											
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 4;
											SET @scriptCode   = 'COD-1900I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = ISNULL(@sqlScript,'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								
								EXEC(@sqlScript);
			
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 4;
											SET @scriptCode   = '';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Hash Key V2 created successfully';
											SET @status       = 'Information';
											SET @SQL          = '';
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 4;
									SET @scriptCode   = 'COD-3200E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
									SET @status       = 'ERROR';
									SET @SQL          = ISNULL(@sqlScript,'');
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,11,1);
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							END CATCH
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Generating Hash V2 Into (' + @dimHashFullObject + ')';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						END
				
				IF(@debug = 1)
					BEGIN
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Incremental Process';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------	
					END
			END
	
	--DROPPING ALL TEMP TABLES
		IF(@continue = 1)
			BEGIN
				IF(OBJECT_ID(@tempHashV1FullObject) IS NOT NULL)
					BEGIN
						SET @sqlScript = 'DROP TABLE ' + @tempHashV1FullObject;
						EXEC(@sqlScript);
					END
				
			END
	
	--RETURN FINAL RESULT
		IF(@continue = 1)
			BEGIN 
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 1;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'COMMIT Transaction';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				COMMIT TRANSACTION
			END
		ELSE
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 1;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'ROLLBACK Transaction';
							SET @status       = 'Information';
							SET @SQL          = '';
							IF(@loggingType IN (1,3))
								BEGIN
									INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
									VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
								END
							IF(@loggingType IN (2,3))
								RAISERROR(@message,10,1);
						END
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
				ROLLBACK TRANSACTION
			END
			
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		IF(@debug = 1)
			BEGIN
				SET @logTreeLevel = 0;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Store Procude';
				SET @status       = 'Information';
				SET @SQL          = '';
				IF(@loggingType IN (1,3))
					BEGIN
						INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
					END
				IF(@loggingType IN (2,3))
					RAISERROR(@message,10,1);
			END
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------

	--Inserting Log into the physical table
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO dbo.BI_log (
					 executionID
					,sequenceID
					,logDateTime
					,object 
					,scriptCode 
					,status 
					,message 
					,SQL 
					,variables 
				)
					SELECT * FROM @BI_log;
			END
	
	--RAISE ERROR IN CASE OF
		IF(@continue = 0)
			BEGIN
				DECLARE @errorMessage NVARCHAR(300);
				
				SET @errorMessage = N'PLEASE CHECK --> SELECT * FROM dbo.BI_log WHERE executionID = ' + CONVERT(NVARCHAR(20),@executionID);
				
				RAISERROR(@errorMessage,11,1);
			END 
END
GO
