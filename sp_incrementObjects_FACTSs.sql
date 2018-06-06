CREATE PROCEDURE [dbo].[sp_incrementObjects_SOURCEs] 
(
	 @schema          NVARCHAR(128)
	,@SRC_table       NVARCHAR(128)
	,@dateColumn      NVARCHAR(128) = NULL
	,@monthsBack      NVARCHAR(3)   = NULL
	,@debug           SMALLINT      = 0
	,@loggingType     SMALLINT      = 1 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
	,@finalTableIsCUR SMALLINT      = 1 --1) The final table will be CUR | 0) The final table will be SRC
)
/*
	Developed by: Mauricio Rivera
	Date: 18 Apr 2018
	
	MODIFICATIONS
		
		
	LAST USED LOGGING IDS:
		- ERRORS      (COD-3200E)
		- INFORMATION (COD-2400I)
*/
AS
BEGIN
	SET NOCOUNT OFF;

	--Transforming input parameter from NULL to ''
		IF(@dateColumn IS NULL)
			SET @dateColumn = '';
		
		IF(@monthsBack IS NULL)
			SET @monthsBack = '';
	
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
				SQL         VARCHAR (MAX) NOT NULL,
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
			,SQL         VARCHAR (MAX)
			,variables   VARCHAR (2500)
		);

	DECLARE
	--PROCESS FLOW VARIABLES
		 @continue              BIT            = 1
		,@sqlScripts            NVARCHAR(MAX)  = N''
		,@INT                   INT            = 0
		,@columns               NVARCHAR(4000) = N''
	--LOGGING VARIABLES
		,@executionID           INT            = NEXT VALUE FOR dbo.sq_BI_log_executionID
		,@execObjectName        VARCHAR(256)   = 'dbo.sp_incrementObjects_SOURCEs'
		,@scriptCode            VARCHAR(25)    = ''
		,@status                VARCHAR(50)    = ''
		,@logTreeLevel          TINYINT        = 0
		,@logSpaceTree          VARCHAR(5)     = '    '
		,@message               VARCHAR(500)   = ''
		,@SQL                   VARCHAR(4000)  = ''
		,@variables             VARCHAR(2500)  = ''
	--FLAGS VARIABLES
		,@changesFound          BIT            = 0
		,@dateColumnSpecified   BIT            = 0
		,@SRC                   BIT            = 0
		,@CUR                   BIT            = 0
		,@HST                   BIT            = 0
		,@reloadProcess         BIT            = 0
	--GENERAL VARIABLES
		,@CUR_table             NVARCHAR(128)  = REPLACE(@SRC_table,N'_SRC',N'_CUR')
		,@HST_table             NVARCHAR(128)  = REPLACE(@SRC_table,N'_SRC',N'_HST')
		,@FNL_table             NVARCHAR(128)  = REPLACE(@SRC_table,N'_SRC',N'')
		,@BIHashColumnName      NVARCHAR(128)  = 'BI_HFR'
		,@BIBeginDateColumnName NVARCHAR(128)  = 'BI_beginDate'
		,@BIEndDateColumnName   NVARCHAR(128)  = 'BI_endDate'
		,@BIBeginDate           DATETIME       = GETDATE()
		,@BIBeginDate_varchar   VARCHAR(50)    = '';
	
	/*------------------------------------------------------------------------------------------------------------------------------
	 ****************************************************************************************************************************** 
	   IF IS A RELOAD PROCESS, CHANGE THE DATE IN THE CONFIG TABLE TO THE PROCESS DATE AND SET TO (1) THE VARIABLE @reloadProcess
	   
	   USE THE FOLLOWING SELECT TO GET THE PARAMETER IN THE CONFIG TABLE
	   		- SELECT value1 FROM dbo.BIConfig WHERE type = 'REPROCESS-DATE-SOURCE';
	   	
	   	USE THE FOLOWING SCRIPT TO UPDATE THE VALUE OF THIS PARAMETER IN THE CONFIG TABLE
	   		- UPDATE INTO dbo.BIConfig SET value1 = '' WHERE type = 'REPROCESS-DATE-SOURCE';
	   
	   THE FORMAT FOR THE VALUE OF THIS VARIABLES IS EG '31 Dec 9999 11:59:59 PM'
	 ******************************************************************************************************************************
	------------------------------------------------------------------------------------------------------------------------------*/
		SET @reloadProcess = 0;
	/*------------------------------------------------------------------------------------------------------------------------------
	 ****************************************************************************************************************************** 
	 ******************************************************************************************************************************
	------------------------------------------------------------------------------------------------------------------------------*/
	
	SET @variables = ' | @schema = '                + ISNULL(CONVERT(VARCHAR(128),@schema               ),'') +
					 ' | @SRC_table = '             + ISNULL(CONVERT(VARCHAR(128),@SRC_table            ),'') +
					 ' | @dateColumn = '            + ISNULL(CONVERT(VARCHAR(128),@dateColumn           ),'') +
					 ' | @monthsBack = '            + ISNULL(CONVERT(VARCHAR(2)  ,@monthsBack           ),'') +
					 ' | @debug = '                 + ISNULL(CONVERT(VARCHAR(1)  ,@debug                ),'') +
					 ' | @loggingType = '           + ISNULL(CONVERT(VARCHAR(1)  ,@loggingType          ),'') +
					 ' | @finalTableIsCUR = '       + ISNULL(CONVERT(VARCHAR(1)  ,@finalTableIsCUR      ),'') +
					 ' | @CUR_table = '             + ISNULL(CONVERT(VARCHAR(1)  ,@CUR_table            ),'') +
					 ' | @HST_table = '             + ISNULL(CONVERT(VARCHAR(1)  ,@HST_table            ),'') +
					 ' | @FNL_table = '             + ISNULL(CONVERT(VARCHAR(1)  ,@FNL_table            ),'') +
					 ' | @BIHashColumnName = '      + ISNULL(CONVERT(VARCHAR(1)  ,@BIHashColumnName     ),'') +
					 ' | @BIBeginDateColumnName = ' + ISNULL(CONVERT(VARCHAR(1)  ,@BIBeginDateColumnName),'') +
					 ' | @BIEndDateColumnName = '   + ISNULL(CONVERT(VARCHAR(1)  ,@BIEndDateColumnName  ),'');
	
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
	
	--CREATING THE ROLLBACK FLAG
		BEGIN TRANSACTION;
		
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
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
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
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Getting BI_endDate parameter from Config Table';
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
				IF(
					EXISTS(
						SELECT 1
						FROM   dbo.BIConfig
						WHERE  type = 'REPROCESS-DATE-SOURCE'
					)
				)
					BEGIN 
						BEGIN TRY
							SET @BIBeginDate = (
								SELECT CONVERT(DATETIME,a.value1)
								FROM dbo.BIConfig a
								WHERE a.type = 'REPROCESS-DATE-SOURCE'
							)
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BI_endDate assigned to (' + CONVERT(VARCHAR(50),@BIBeginDate,100) + ')';
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
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-100E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error ocurs while trying to convert the value from config table to Datetime';
								SET @status       = 'ERROR';
								SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');;
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
				ELSE
					BEGIN
						SET @continue = 0;
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-200E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Reload Process Activated. However, the config table does not have value';
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
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Getting BI_beginDate parameter from Config Table';
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
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN validation of BI_beginDate';
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
				
				IF(DATEPART(hour,@BIBeginDate) = 0)
					BEGIN
						SET @continue = 0;
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							SET @logTreeLevel = 3;
							SET @scriptCode   = 'COD-300E';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The date specified in the config table is not valid (' + CONVERT(VARCHAR(50),@BIBeginDate,100) + '). Please include the time part in ''11:59:50 PM'' format.';
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
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END validation of BI_endDate';
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
				SET @BIBeginDate_varchar = CONVERT(VARCHAR(50),@BIBeginDate,100);
			END 
	
	--IN CASE OF @monthsBack IS NOT SPECIFIED, GET IT FROM THE BI CONFIG TABLE
		IF(@continue = 1 AND LEN(RTRIM(LTRIM(@monthsBack))) = 0)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Input parameter @monthsBack not provided. Looking for MONTHS-BACK parameter from the BIConfig table';
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
				
				IF(
					EXISTS(
						SELECT 1
						FROM dbo.BIConfig
						WHERE 
							    type     = 'MONTHS-BACK'
							AND disabled = 0
					)
				)
					BEGIN
						SELECT 
							@monthsBack = value1
						FROM dbo.BIConfig
						WHERE 
							    type     = 'MONTHS-BACK'
							AND disabled = 0;
					
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Input parameter @monthsBack has changed to ' + @monthsBack;
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
			END
	
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
						VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
					END
				IF(@loggingType IN (2,3))
				   	RAISERROR(@message,10,1);
			END 
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			
		IF(@schema IS NULL OR LEN(RTRIM(LTRIM(@schema))) = 0)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-400E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Parameter @schema can not be empty or NULL';
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
		ELSE IF(@SRC_table IS NULL OR LEN(RTRIM(LTRIM(@SRC_table))) = 0)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-500E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Input Parameter @SRC_table can not be empty or NULL';
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
		ELSE IF(ISNUMERIC(@monthsBack) = 0)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-600E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'If The Input Parameter @monthsBack is specified. @monthsBack should be numeric';
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
		ELSE IF(@finalTableIsCUR < 0 OR @finalTableIsCUR > 1)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-700E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The imput parameter @finalTableIsCUR must be (1) or (0)';
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
	
	--VALIDATING SRC TABLE
		IF(@continue = 1)
			BEGIN
				IF(
					OBJECT_ID(@schema + N'.' + @SRC_table) IS NULL
					OR EXISTS(
						SELECT 1
						FROM sys.objects a
						WHERE
							a.object_id = OBJECT_ID(@schema + N'.' + @SRC_table)
							AND a.type NOT IN ('U','V')
					)
				)
					BEGIN
						SET @continue = 0;
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							SET @logTreeLevel = 3;
							SET @scriptCode   = 'COD-800E';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The SRC table does not exist or is not a TABLE or VIEW';
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
			END
	
	--IF @dateColumn IS SPECIFIED, VALIDATE ITS EXISTENCE ON THE SRC TABLE
		IF(@continue = 1 AND LEN(RTRIM(LTRIM(@dateColumn))) > 0)
			BEGIN
				IF(
					NOT EXISTS(
						SELECT 1 
							FROM 
								sys.columns a INNER JOIN sys.types b ON
									    b.system_type_id = a.system_type_id
									AND b.user_type_id   = a.user_type_id
							WHERE 
							   	    a.OBJECT_ID = OBJECT_ID(@schema + N'.' + @SRC_table)
								AND a.name      = @dateColumn
								AND b.name     IN ('datetime','smalldatetime','datetime2')
					)
				)
					BEGIN
						SET @continue = 0;
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							SET @logTreeLevel = 3;
							SET @scriptCode   = 'COD-900E';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Input Parameter @dateColumn does not exist on the SRT (Source) table or has not a valid DateTime data type';
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
						SET @dateColumnSpecified = 1;
					END
			END
	
	--IF DATE COLUMN IS ESPECIFIED, MONTHS BACK PARAMETER IS REQUIRED
		IF(@continue = 1 AND @dateColumnSpecified = 1 AND (ISNUMERIC(@monthsBack) = 0 OR @monthsBack = 0))
			BEGIN
				SET @continue            = 0;
				SET @dateColumnSpecified = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = 3;
					SET @scriptCode   = 'COD-1000E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Input Parameter @monthsBack is required when the parameter @dateColumn is specified';
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
		IF(@debug = 1 AND @continue = 1)
			BEGIN
				SET @logTreeLevel = 3;
				SET @scriptCode   = '';
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Validation Success';
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
				SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Input Parameter Validation';
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
	
	--Validation: SRC (Source) object exist or is empty
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Validating @SRC_table exist or it is empty';
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
					SET @SRC        = 0;
					SET @sqlScripts = N'SELECT DISTINCT @exist = 1 FROM ' + @schema + N'.' + @SRC_table;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-100I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					EXEC sp_executesql @sqlScripts, N'@exist SMALLINT OUTPUT', @exist = @SRC OUTPUT;
					
					IF (@SRC = 0)
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1100E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The table is empty';
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
				END TRY
				BEGIN CATCH
					SET @SRC      = 0;							
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1200E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The table does not exist';
						SET @status       = 'ERROR';
						SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
					IF(@debug = 1 AND @continue = 1)
						BEGIN
							SET @logTreeLevel = 3;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Validation Success';
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
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Validating @SRC_table exist or it is empty';
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
	
	--Validate duplicity of the HFR in the SRC table 
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Validating duplicated BI_HFR rows in the SRC table';
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
					SET @sqlScripts = N'EXEC dbo.sp_validateDuplicatedRows ''' + @schema + ''',''' + @SRC_table + ''',''' + @BIHashColumnName + ''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
								
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-200I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
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
								SET @scriptCode   = 'COD-1300E';
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
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Validating duplicated BI_HFR rows in the SRC table';
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
				
	--Second Validation: CUR (Current) object exist
		IF(@continue = 1)
			BEGIN 
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Validating @CUR_table exist or it is empty';
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
					SET @CUR        = 0;
					SET @sqlScripts = N'SELECT DISTINCT @exist = 1 FROM ' + @schema + N'.' + @CUR_table;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-300I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					EXEC sp_executesql @sqlScripts, N'@exist SMALLINT OUTPUT', @exist = @CUR OUTPUT;
					
					IF (@CUR = 0) --If no error happens, the table exist. However, if @CUR is 0, the table is empty. That's why we change it to 1
						BEGIN
							SET @CUR = 1;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The table is empty';
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
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The table does not exist';
								SET @status       = 'Information';
								SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Checking if Previously loaded FNL_table exists';
								SET @status       = 'Information';
								SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					IF(
						EXISTS(
							SELECT 1
							FROM sys.objects
							WHERE object_id = OBJECT_ID(@schema + N'.' + @FNL_table)
						)
					)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'FNL_table found';
										SET @status       = 'Information';
										SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Renaming FNL_table to CUR_table';
										SET @status       = 'Information';
										SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
							SET @sqlScripts = N'sp_rename ''' + @schema + N'.' + @FNL_table + N''', ''' + @CUR_table + N'''';
						
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-400I';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
										SET @status       = 'Information';
										SET @SQL          = @sqlScripts;
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
							EXEC(@sqlScripts)
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'FNL_table has been renamed sucessfully';
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
							
							SET @CUR = 1;
						END
					ELSE
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'FNL_table not found';
										SET @status       = 'Information';
										SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
							SET @CUR = 0;
						END
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1 AND @continue = 1)
						BEGIN
							SET @logTreeLevel = 3;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Validation Success';
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
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Validating @CUR_table exist or it is empty';
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
	
	--Third Validation: HST (History) object exist
		IF(@continue = 1)
			BEGIN 
				BEGIN TRY
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Validating @HST_table exist or it is empty';
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
					
					SET @HST        = 0;
					SET @sqlScripts = N'SELECT DISTINCT @exist = 1 FROM ' + @schema + N'.' + @HST_table;
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-500I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScripts, N'@exist SMALLINT OUTPUT', @exist = @HST OUTPUT;
					
					IF (@HST = 0) --If no error happens, the table exist. However, if @HST is 0, the table is empty. That's why we change it to 1
						BEGIN
							SET @HST = 1;
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The table is empty';
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
					SET @HST = 0;
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The table does not exist';
								SET @status       = 'Information';
								SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
					IF(@debug = 1 AND @continue = 1)
						BEGIN
							SET @logTreeLevel = 3;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Validation Success';
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
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Validating @HST_table exist or it is empty';
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
	
	--If CUR (Current) Table does not exist
		IF(@continue = 1 AND @CUR = 0)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Creating Table ' + @schema + '.' + @CUR_table;
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
					SET @sqlScripts = N'SELECT *, ''' + @BIBeginDate_varchar + ''' AS ' + @BIBeginDateColumnName + ' INTO ' + @schema + N'.' + @CUR_table + N' FROM ' + @schema + N'.' + @SRC_table + ' (NOLOCK)';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-600I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC(@sqlScripts);
					SET @CUR = 1;
					
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
					
					SET @sqlScripts = N'SELECT DISTINCT @count = COUNT(*) FROM ' + @schema + N'.' + @SRC_table;				
						
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 4;
								SET @scriptCode   = 'COD-700I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScripts, N'@count INT OUTPUT', @count = @INT OUTPUT;
					
					IF(@INT IS NULL OR @INT = 0)
						BEGIN
							SET @continue = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								SET @logTreeLevel = 4;
								SET @scriptCode   = 'COD-1400E';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The table CUR does not have records after its creation';
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
						SET @scriptCode   = 'COD-1500E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create the CUR table';
						SET @status       = 'ERROR';
						SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Creating Table ' + @schema + '.' + @CUR_table;
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
	
	--If HST (History) Table does not exist
		IF(@continue = 1 AND @HST = 0)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Creating Table ' + @schema + '.' + @HST_table;
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
					SET @sqlScripts = N'SELECT *, GETDATE() AS ' + @BIBeginDateColumnName + ', GETDATE() AS ' + @BIEndDateColumnName + ' INTO ' + @schema + N'.' + @HST_table + N' FROM ' + @schema + N'.' + @SRC_table + N' (NOLOCK) WHERE 1 = 0';
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-800I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC(@sqlScripts);
					SET @HST = 1;  
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Table Created Sucessfully';
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
					SET @continue = 0;
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						SET @logTreeLevel = 3;
						SET @scriptCode   = 'COD-1600E';
						SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to create the HST (History) table';
						SET @status       = 'ERROR';
						SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Creating Table ' + @schema + '.' + @HST_table;
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
	
	--Third Preparation: Homogenizing CUR (Current) table columns with SRC (Source) table columns
		IF(@continue = 1 AND @SRC = 1 AND @CUR = 1)
			BEGIN 
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Homogenizing CUR table columns';
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
					SET @sqlScripts = N'EXEC dbo.sp_homogeniseObjectStructure @objectFrom = ''' + @schema + N'.' + @SRC_table + ''', @objectTo = ''' + @schema + N'.' + @CUR_table + ''', @addNewColumns = 1, @dropNonUsedColumns = 0, @alterDataType = 1, @dontLoseDataWhenDataTypeChange = 1, @status = @statusInt OUTPUT, @message = @messageInt OUTPUT, @SQL = @SQLInt OUTPUT';

					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-900I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(4000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
										
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
								SET @scriptCode   = 'COD-1700E';
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
				END CATCH
								
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Homogenizing CUR table columns';
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
	
	--Fourth Preparation: Homogenizing HST (History) table columns with SRC (Source) table columns
		IF(@continue = 1 AND @SRC = 1 AND @HST = 1)
			BEGIN				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Homogenizing HST Columns';
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
					SET @sqlScripts = N'EXEC dbo.sp_homogeniseObjectStructure @objectFrom = ''' + @schema + N'.' + @SRC_table + ''', @objectTo = ''' + @schema + N'.' + @HST_table + ''', @addNewColumns = 1, @dropNonUsedColumns = 0, @alterDataType = 1, @dontLoseDataWhenDataTypeChange = 1, @status = @statusInt OUTPUT, @message = @messageInt OUTPUT, @SQL = @SQLInt OUTPUT';

					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = 3;
								SET @scriptCode   = 'COD-1000I';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
								SET @status       = 'Information';
								SET @SQL          = @sqlScripts;
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(4000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
					
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
				END CATCH
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Homogenizing HST Columns';
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
	
	--Fifth Preparation: Cheking Indexes
		IF(@continue = 1 AND @SRC = 1 AND @HST = 1)
			BEGIN
				--INDEX OVER HST TABLE ON @BIHashColumnName COLUMN
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Indexes on HST table over column ' + @BIHashColumnName;
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
								SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 2, ''DL_NC_' + @schema + @HST_table + N'_' + @BIHashColumnName + ''',''' + @schema + ''',''' + @HST_table + ''',''' + @BIHashColumnName + ''','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1100I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = @sqlScripts;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								
								EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
								
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
											SET @scriptCode   = 'COD-1900E';
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
							END CATCH
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------g
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Indexes on HST table over column ' + @BIHashColumnName;
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

				--INDEX OVER CUR TABLE ON @BIHashColumnName COLUMN
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Indexes on CUR table over column ' + @BIHashColumnName;
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
								SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 2, ''DL_NC_' + @schema + @CUR_table + N'_' + @BIHashColumnName + ''',''' + @schema + ''',''' + @CUR_table + ''',''' + @BIHashColumnName + ''','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1200I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = @sqlScripts;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								
								EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
								
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
							END CATCH
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Indexes on CUR table over column ' + @BIHashColumnName;
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
				
				--SRC (SOURCE) TABLE INDEX
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Indexes on SRC table over column ' + @BIHashColumnName;
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
								SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 2, ''DL_NC_' + @schema + @SRC_table + N'_' + @BIHashColumnName + ''',''' + @schema + ''',''' + @SRC_table + ''',''' + @BIHashColumnName + ''','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1300I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = @sqlScripts;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								
								EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
								
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
											SET @scriptCode   = 'COD-2100E';
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
							END CATCH
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Indexes on SRC table over column ' + @BIHashColumnName;
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
						
				--DATE COLUMN INDEX	FOR SRC
					IF(@continue = 1 AND @dateColumnSpecified = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Indexes on SRC table over column ' + @dateColumn;
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
								SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 2, ''DL_NC_' + @schema + @SRC_table + N'_' + @dateColumn + ''',''' + @schema + ''',''' + @SRC_table + ''',''' + @dateColumn + ''','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1400I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = @sqlScripts;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								
								EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
								
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
							END CATCH
								
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Indexes on SRC table over column ' + @dateColumn;
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
						
				--DATE COLUMN INDEX	FOR HST
					IF(@continue = 1 AND @dateColumnSpecified = 1)
						BEGIN 
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Indexes on HST table over column ' + @dateColumn;
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
								SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 2, ''DL_NC_' + @schema + @HST_table + N'_' + @dateColumn + ''',''' + @schema + ''',''' + @HST_table + ''',''' + @dateColumn + ''','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1500I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = @sqlScripts;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								
								EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
								
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
											SET @scriptCode   = 'COD-2300E';
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
							END CATCH
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Indexes on HST table over column ' + @dateColumn;
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
						
				--DATE COLUMN INDEX	FOR CUR
					IF(@continue = 1 AND @dateColumnSpecified = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Checking Indexes on CUR table over column ' + @dateColumn;
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
								SET @sqlScripts = N'EXEC dbo.sp_manageIndexes 1, 2, ''DL_NC_' + @schema + @CUR_table + N'_' + @dateColumn + ''',''' + @schema + ''',''' + @CUR_table + ''',''' + @dateColumn + ''','''',@statusInt OUTPUT, @messageInt OUTPUT, @SQLInt OUTPUT';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1600I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = @sqlScripts;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								
								EXEC sp_executesql @sqlScripts, N'@statusInt TINYINT OUTPUT,@messageInt NVARCHAR(500) OUTPUT,@SQLInt VARCHAR(1000) OUTPUT', @statusInt = @continue OUTPUT, @messageInt = @message OUTPUT, @SQLInt = @SQL OUTPUT;
								
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
							END CATCH
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 2;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Checking Indexes on CUR table over column ' + @dateColumn;
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
			END

	--First Incremental Process: HST (History) Table
		IF(@continue = 1 AND @SRC = 1 AND @HST = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Incremental process for HST table';
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
							
							BEGIN TRY
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
								
								IF OBJECT_ID ('tempdb..##incrementObjects_changeDelete') IS NOT NULL
									DROP TABLE ##incrementObjects_changeDelete;	
								
										SET @sqlScripts =               N'SELECT ';
										SET @sqlScripts = @sqlScripts +     N'a.' + @BIHashColumnName + ' ';
										SET @sqlScripts = @sqlScripts + N'INTO ';
										SET @sqlScripts = @sqlScripts +     N'##incrementObjects_changeDelete ';
										SET @sqlScripts = @sqlScripts + N'FROM ';
										SET @sqlScripts = @sqlScripts +     @schema + N'.' + @CUR_table + N' a LEFT JOIN ' + @schema + N'.' + @SRC_table + N' b ON ';
										SET @sqlScripts = @sqlScripts +         N'b.' + @BIHashColumnName + ' = a.' + @BIHashColumnName + ' ';
										SET @sqlScripts = @sqlScripts + N'WHERE ';
										SET @sqlScripts = @sqlScripts +     N'b.' + @BIHashColumnName + ' IS NULL ';
								IF(@dateColumnSpecified = 1)
									BEGIN
										SET @sqlScripts = @sqlScripts +     N' AND a.' + @dateColumn + ' >= DATEADD(MONTH,' + @monthsBack + ',CONVERT(DATETIME,''' + @BIBeginDate_varchar + '''))';
									END
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1700I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = @sqlScripts;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									
								EXEC(@sqlScripts);
								
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
											IF(@debug = 1)
												BEGIN
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
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END
								
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								
								IF OBJECT_ID ('tempdb..##incrementObjects_changeDelete') IS NOT NULL
									DROP TABLE ##incrementObjects_changeDelete;
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-2500E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to generate the difference data between CUR table and SRC table';
									SET @status       = 'ERROR';
									SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
				
				--INSERTING CHANGED / DELETED ROWS INTO HST TABLE
					IF(@continue = 1 AND @changesFound = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Inserting changed / deleted rows into HST table';
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
								SET @sqlScripts =               N'INSERT INTO ' + @schema + N'.' + @HST_table + N' ';
								SET @sqlScripts = @sqlScripts +     N'SELECT ';
								SET @sqlScripts = @sqlScripts +         N'b.* ';
								SET @sqlScripts = @sqlScripts +         N',''' + @BIBeginDate_varchar + ''' AS ' + @BIEndDateColumnName + ' ';
								SET @sqlScripts = @sqlScripts +     N'FROM ';
								SET @sqlScripts = @sqlScripts +         N'##incrementObjects_changeDelete a INNER JOIN ' + @schema + N'.' + @CUR_table + N' (NOLOCK) b ON ';
								SET @sqlScripts = @sqlScripts +             N'b.' + @BIHashColumnName + ' = a.' + @BIHashColumnName + ' ';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1800I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = @sqlScripts;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END 
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
	
								EXEC(@sqlScripts);
								
								SET @INT = @@ROWCOUNT;
								
								IF(@INT > 0)
									BEGIN 
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
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
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-2600E';
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
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								
								IF OBJECT_ID ('tempdb..##incrementObjects_changeDelete') IS NOT NULL
									DROP TABLE ##incrementObjects_changeDelete;
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-2700E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to insert the changes/deletes rows into the HST table';
									SET @status       = 'ERROR';
									SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
				
				--DELETING CHANGED / DELETED ROWS IN CUR TABLE
					IF(@continue = 1 AND @changesFound = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Deleting changed / deleted rows in CUR table';
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
								SET @sqlScripts =               N'DELETE a ';
								SET @sqlScripts = @sqlScripts + N'FROM ';
								SET @sqlScripts = @sqlScripts +     @schema + N'.' + @CUR_table + N' a INNER JOIN ##incrementObjects_changeDelete b ON ';
								SET @sqlScripts = @sqlScripts +     N'b.' + @BIHashColumnName + ' = a.' + @BIHashColumnName + ' ';
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									IF(@debug = 1)
										BEGIN
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-1900I';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
											SET @status       = 'Information';
											SET @SQL          = @sqlScripts;
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,10,1);
										END
								----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								
								EXEC(@sqlScripts)
								
								SET @INT = @@ROWCOUNT;
								
								IF(@INT > 0)
									BEGIN 
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
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
											SET @logTreeLevel = 3;
											SET @scriptCode   = 'COD-2800E';
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
								
								IF OBJECT_ID ('tempdb..##incrementObjects_changeDelete') IS NOT NULL
									DROP TABLE ##incrementObjects_changeDelete;
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								
								IF OBJECT_ID ('tempdb..##incrementObjects_changeDelete') IS NOT NULL
									DROP TABLE ##incrementObjects_changeDelete;
								
								----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-2900E';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to delete the changes/deletes rows in the CUR table';
									SET @status       = 'ERROR';
									SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
						
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Incremental process for HST table';
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
	
	--Second Incremental Process: CUR (Current) Table
		IF(@continue = 1 AND @SRC = 1 AND @CUR = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Incremental process for CUR table';
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
				
				--GETTING NEW ROWS
					IF(@continue = 1)
						BEGIN
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Getting New Rows';
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
						
							SET @changesFound = 0;
							
							--GETTING NEW ROWS
								BEGIN TRY
									IF OBJECT_ID ('tempdb..##incrementObjects_new') IS NOT NULL
										DROP TABLE ##incrementObjects_new;	
									
									SET @sqlScripts =               N'SELECT '
									SET @sqlScripts = @sqlScripts +     N'a.' + @BIHashColumnName + ' ';
									SET @sqlScripts = @sqlScripts + N'INTO ';
									SET @sqlScripts = @sqlScripts +     N'##incrementObjects_new ';
									SET @sqlScripts = @sqlScripts + N'FROM ';
									SET @sqlScripts = @sqlScripts +     @schema + N'.' + @SRC_table + N' a LEFT JOIN ' + @schema + N'.' + @CUR_table + N' b ON ';
									SET @sqlScripts = @sqlScripts +         N'b.' + @BIHashColumnName + ' = a.' + @BIHashColumnName + ' ';
									SET @sqlScripts = @sqlScripts + N'WHERE ';
									SET @sqlScripts = @sqlScripts +     N'b.' + @BIHashColumnName + ' IS NULL ';
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = 3;
												SET @scriptCode   = 'COD-2000I';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
												SET @status       = 'Information';
												SET @SQL          = @sqlScripts;
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@message,10,1);
											END
									----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										
									EXEC(@sqlScripts);
									
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
												IF(@debug = 1)
													BEGIN
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
													END
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										END
									
								END TRY
								BEGIN CATCH
									SET @continue = 0;
									
									IF OBJECT_ID ('tempdb..##incrementObjects_new') IS NOT NULL
										DROP TABLE ##incrementObjects_new;
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-3000E';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to get new rows';
										SET @status       = 'ERROR';
										SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),N'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,11,1);
									----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								END CATCH
							
							--INSERTING NEW ROWS INTO CUR TABLE
								IF(@continue = 1 AND @changesFound = 1)
									BEGIN
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = 3;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Inserting new data into CUR table';
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
											--GETTINGS COLUMNS CUR TABLE
												SET @columns = (
													SELECT
														STUFF(
															(
																SELECT 
																	N',[' + a.name + N']'
																FROM
																	sys.columns a
																WHERE
																	a.object_id = OBJECT_ID(@schema + N'.' + @CUR_table)
																ORDER BY
																	a.column_id ASC
																FOR XML PATH(''), TYPE
															).value('.', 'VARCHAR(MAX)'), 1, 1, ''
														)
												);
										--INSERT - 1
											SET @sqlScripts =               N'INSERT INTO ' + @schema + N'.' + @CUR_table + N' (' + @columns + N') ';
											
											--GETTINGS COLUMNS SRC TABLE
												SET @columns = (
													SELECT
														STUFF(
															(
																SELECT 
																	N',b.[' + a.name + N']'
																FROM
																	sys.columns a INNER JOIN sys.columns b ON
																		    b.object_id = OBJECT_ID(@schema + N'.' + @CUR_table)
																		AND a.object_id = OBJECT_ID(@schema + N'.' + @SRC_table)
																		AND b.name = a.name
																ORDER BY
																	b.column_id ASC
																FOR XML PATH(''), TYPE
															).value('.', 'VARCHAR(MAX)'), 1, 1, ''
														)
												);
										--INSERT - 2
											SET @sqlScripts = @sqlScripts +     N'SELECT ';
											SET @sqlScripts = @sqlScripts +         @columns;
											SET @sqlScripts = @sqlScripts +         N',''' + @BIBeginDate_varchar + ''' AS ' + @BIBeginDateColumnName + ' ';
											SET @sqlScripts = @sqlScripts +     N'FROM ';
											SET @sqlScripts = @sqlScripts +         N'##incrementObjects_new a INNER JOIN ' + @schema + N'.' + @SRC_table + N' (NOLOCK) b ON ';
											SET @sqlScripts = @sqlScripts +             N'b.' + @BIHashColumnName + ' = a.' + @BIHashColumnName + ' ';
											
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = 3;
														SET @scriptCode   = 'COD-2100I';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
														SET @status       = 'Information';
														SET @SQL          = @sqlScripts;
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sqlScripts,@variables);
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,10,1);
													END
											----------------------------------------------------- END INSERT LOG ----------------------------------------------------- 

											EXEC(@sqlScripts);
											
											SET @INT = @@ROWCOUNT;
											
											IF(@INT > 0)
												BEGIN 
													----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
														IF(@debug = 1)
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
														SET @logTreeLevel = 3;
														SET @scriptCode   = 'COD-3100E';
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
												
											IF OBJECT_ID ('tempdb..##incrementObjects_new') IS NOT NULL
												DROP TABLE ##incrementObjects_new;
										END TRY
										BEGIN CATCH
											SET @continue = 0;
											
											IF OBJECT_ID ('tempdb..##incrementObjects_new') IS NOT NULL
												DROP TABLE ##incrementObjects_new;
											
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												SET @logTreeLevel = 3;
												SET @scriptCode   = 'COD-3200E';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'An error occurred while trying to insert the new rows into the CUR table';
												SET @status       = 'ERROR';
												SET @SQL          = 'SQL Error: line(' + ISNULL(CONVERT(VARCHAR(20),ERROR_LINE()),'') + ') - Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
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
						END
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Incremental process for CUR table';
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
			
	--CREATION OF THE FINAL TABLE TO BE CONSUMED BY THE TRANSFORMATION LAYER
		IF(@continue = 1)
			BEGIN
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Creation of final table';
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
				
				IF OBJECT_ID (@schema + N'.' + @FNL_table) IS NOT NULL
					BEGIN
						--CREATING AUTO BACKUP OF FINAL TABLE
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Creating Auto Backup of the Final Table';
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
							
							IF OBJECT_ID (@schema + N'.' + @FNL_table + '_autoBackup') IS NOT NULL
								BEGIN
									SET @sqlScripts = 'DROP TABLE ' + @schema + N'.' + @FNL_table + '_autoBackup'; 
									EXEC(@sqlScripts);	
								END
							
							SET @sqlScripts =               N'SELECT * ';
							SET @sqlScripts = @sqlScripts + N'INTO ';
							SET @sqlScripts = @sqlScripts +     @schema + N'.' + @FNL_table + '_autoBackup ';
							SET @sqlScripts = @sqlScripts + N'FROM ';
							SET @sqlScripts = @sqlScripts +     @schema + N'.' + @FNL_table + ' ';
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = 'COD-2200I';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
										SET @status       = 'Information';
										SET @SQL          = @sqlScripts;
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END 
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			
							EXEC(@sqlScripts);
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Creating Auto Backup of the Final Table';
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
						
						--DROP FINAL TABLE	
							SET @sqlScripts = 'DROP TABLE ' + @schema + N'.' + @FNL_table;
							EXEC(@sqlScripts);
					END
					
				IF(@finalTableIsCUR = 1)
					BEGIN
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Renaming CUR_table with Final Table name';
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
						
						SET @sqlScripts = N'sp_rename ''' + @schema + N'.' + @CUR_table + N''', ''' + @FNL_table + N'''';
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-2300I';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
									SET @status       = 'Information';
									SET @SQL          = @sqlScripts;
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,10,1);
								END
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						
						EXEC(@sqlScripts)
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'CUR_table has been renamed sucessfully';
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
				ELSE IF(@finalTableIsCUR = 0)
					BEGIN
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Renaming SRC_table table with Final Table name';
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
						
						SET @sqlScripts = N'sp_rename ''' + @schema + N'.' + @SRC_table + N''', ''' + @FNL_table + N'''';
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = 'COD-2400I';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Executing SQL script';
									SET @status       = 'Information';
									SET @SQL          = @sqlScripts;
									IF(@loggingType IN (1,3))
										BEGIN
											INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
											VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
										END
									IF(@loggingType IN (2,3))
										RAISERROR(@message,10,1);
								END
						----------------------------------------------------- END INSERT LOG -----------------------------------------------------
						
						EXEC(@sqlScripts)
						
						----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
							IF(@debug = 1)
								BEGIN
									SET @logTreeLevel = 3;
									SET @scriptCode   = '';
									SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SRC_table has been renamed sucessfully';
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
				
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					IF(@debug = 1)
						BEGIN
							SET @logTreeLevel = 2;
							SET @scriptCode   = '';
							SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Creation of final table';
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
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		
	--Inserting Log into the physical table
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO dbo.BI_log
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
