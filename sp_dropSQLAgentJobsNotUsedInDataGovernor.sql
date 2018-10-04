CREATE PROCEDURE dbo.sp_dropSQLAgentJobsNotUsedInDataGovernor AS
BEGIN
	DECLARE
		 @continue           BIT           = 1
		,@message            VARCHAR(300)  = ''
		,@dataGovernorDBName NVARCHAR(128) = N''
		,@SQLscript          NVARCHAR(MAX) = N''
		,@jobId_C            NVARCHAR(128) = N'';
	
	IF(@@SERVERNAME = 'CHEBIZ1')--PRODUCTION
		BEGIN
			SET @dataGovernorDBName = 'PerspectiveDataGovernorProduction'
		END
	ELSE IF(@@SERVERNAME = 'CHEVBI4')--TEST
		BEGIN
			SET @dataGovernorDBName = 'PerspectiveDataGovernorTest'
		END
	ELSE IF(@@SERVERNAME = 'CHEVBI3')--DEV
		BEGIN
			SET @dataGovernorDBName = 'PerspectiveDataGovernorDevelopment'
		END
	
	IF(LEN(@dataGovernorDBName) = 0)
		BEGIN
			SET @continue = 0;
		END
	
	IF(@continue = 1)
		BEGIN
			BEGIN TRY
				IF (SELECT CURSOR_STATUS('GLOBAL','DSQLAJNUIDG_cursor')) >= -1
					BEGIN
						DEALLOCATE DSQLAJNUIDG_cursor;
					END
					
				SET @SQLscript = '
					DECLARE DSQLAJNUIDG_cursor CURSOR GLOBAL FOR
						SELECT 
							CONVERT(NVARCHAR(128),a.job_id)
						FROM 
							msdb.dbo.sysjobs a LEFT JOIN ' +  @dataGovernorDBName + '.ProcessGroup b ON
								b.AgentJobID = a.job_id
						WHERE
							    b.AgentJobID IS NULL
							AND a.name       LIKE ''DataGovernor (%''
				';

				EXEC(@SQLscript);
			END TRY
			BEGIN CATCH
				SET @continue = 0;
				SET @message  = 'SQL Error: COD-01E - ' + ISNULL(ERROR_MESSAGE(),'');
				RAISERROR(@message,11,1);
			END CATCH
		END
	
	IF(@continue = 1)
		BEGIN
			SET @SQLscript = N'';
			
			OPEN DSQLAJNUIDG_cursor;
	
			FETCH NEXT FROM DSQLAJNUIDG_cursor INTO @jobId_C;
			
			WHILE (@@FETCH_STATUS = 0 AND @continue = 1)
				BEGIN
					BEGIN TRY
						SET @SQLscript = N'EXEC msdb.dbo.sp_delete_job @job_id = ''' + @jobId_C + '''';
						EXEC(@SQLscript);
						
						FETCH NEXT FROM DSQLAJNUIDG_cursor INTO @jobId_C;
					END TRY
					BEGIN CATCH
						SET @continue = 0;
						SET @message  = 'SQL Error: COD-02E - ' + ISNULL(ERROR_MESSAGE(),'');
						RAISERROR(@message,11,1);
					END CATCH
				END
			
			CLOSE DSQLAJNUIDG_cursor;
		END
	
	IF (SELECT CURSOR_STATUS('GLOBAL','DSQLAJNUIDG_cursor')) >= -1
		BEGIN
			DEALLOCATE DSQLAJNUIDG_cursor;
		END
END
