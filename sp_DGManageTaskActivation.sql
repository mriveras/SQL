CREATE PROCEDURE dbo.sp_DGManageTaskActivation
	(
		 @idType  VARCHAR(50) = NULL
		,@DG_id   INT         = NULL
		,@active  BIT         = NULL
		,@status  BIT                OUTPUT 
		,@message VARCHAR(1000)      OUTPUT
		,@SQL     VARCHAR(4000)      OUTPUT
	)
AS
BEGIN
	SET @idType  = UPPER(@idType);
	SET @SQL     = N'';
	SET @message = '';
	
	DECLARE
		 @continue             BIT           = 1
		,@sqlScript            NVARCHAR(MAX) = N''
		,@SMALLINT             SMALLINT      = 0
		,@DG_serverName        VARCHAR(128)  = ''
		,@DG_dataBaseName      VARCHAR(128)  = '';
	
	SET @DG_serverName   = dbo.udf_getBIConfigParameter('DATAGOVERNOR-DETAILS',1);
	SET @DG_dataBaseName = dbo.udf_getBIConfigParameter('DATAGOVERNOR-DETAILS',2);
	
	--CHEK INPUT VARIABLES
		IF(@idType NOT IN ('GROUP','PROJECT','JOB','TASK'))
			BEGIN
				SET @continue = 0;
				SET @message  = 'ERROR - The parameter @idType only accept (GROUP,PROJECT,JOB or TASK) as value';
			END
		ELSE IF(@active IS NULL)
			BEGIN
				SET @continue = 0;
				SET @message  = 'ERROR - The parameter @active only accept (1 or 0) as value';
			END
	
	--VALIDATE @DG_id
		IF(@continue = 1)
			BEGIN
				IF(@idType = 'GROUP')
					BEGIN
						BEGIN TRY
							SET @SMALLINT = 0;
							SET @sqlScript = 'SELECT @exist = 1 FROM ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.SecurityGroup a WHERE a.GroupID = ' + CONVERT(VARCHAR(10),@DG_id);
							EXEC sp_executesql @sqlScript, N'@exist SMALLINT OUTPUT', @exist = @SMALLINT OUTPUT;
							IF(@SMALLINT IS NULL OR @SMALLINT = 0)
								BEGIN
									SET @continue = 0;
									SET @message  = 'ERROR - The parameter @DG_id (' + CONVERT(VARCHAR(10),@DG_id) + ') has an invalid Group Id';
								END	
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'Error - Wrong Execution'
							SET @SQL      = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
				ELSE IF(@idType = 'PROJECT')
					BEGIN
						BEGIN TRY
							SET @SMALLINT = 0;
							SET @sqlScript = 'SELECT @exist = 1 FROM ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.SecurityProject a WHERE a.ProjectID = ' + CONVERT(VARCHAR(10),@DG_id);
							EXEC sp_executesql @sqlScript, N'@exist SMALLINT OUTPUT', @exist = @SMALLINT OUTPUT;
							IF(@SMALLINT IS NULL OR @SMALLINT = 0)
								BEGIN
									SET @continue = 0;
									SET @message  = 'ERROR - The parameter @DG_id (' + CONVERT(VARCHAR(10),@DG_id) + ') has an invalid Project Id';
								END	
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'Error - Wrong Execution'
							SET @SQL      = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
				ELSE IF(@idType = 'JOB')
					BEGIN
						BEGIN TRY
							SET @SMALLINT = 0;
							SET @sqlScript = 'SELECT @exist = 1 FROM ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessGroup a WHERE a.ProcessGroupID = ' + CONVERT(VARCHAR(10),@DG_id);
							EXEC sp_executesql @sqlScript, N'@exist SMALLINT OUTPUT', @exist = @SMALLINT OUTPUT;
							IF(@SMALLINT IS NULL OR @SMALLINT = 0)
								BEGIN
									SET @continue = 0;
									SET @message  = 'ERROR - The parameter @DG_id (' + CONVERT(VARCHAR(10),@DG_id) + ') has an invalid Job Id';
								END	
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'Error - Wrong Execution'
							SET @SQL      = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
				ELSE IF(@idType = 'TASK')
					BEGIN
						BEGIN TRY				
							SET @SMALLINT = 0;
							SET @sqlScript = 'SELECT @exist = 1 FROM ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.Process a WHERE a.ProcessID = ' + CONVERT(VARCHAR(10),@DG_id);
							EXEC sp_executesql @sqlScript, N'@exist SMALLINT OUTPUT', @exist = @SMALLINT OUTPUT;
							IF(@SMALLINT IS NULL OR @SMALLINT = 0)
								BEGIN
									SET @continue = 0;
									SET @message  = 'ERROR - The parameter @DG_id (' + CONVERT(VARCHAR(10),@DG_id) + ') has an invalid Task Id';
								END	
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'Error - Wrong Execution'
							SET @SQL      = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
			END
	
	--PROCEED TO TACTIVATE OR DEACTIVATE TASKS
		IF(@continue = 1)
			BEGIN
				BEGIN TRANSACTION
				IF(@idType = 'GROUP')
					BEGIN
						BEGIN TRY
							SET @sqlScript =   'UPDATE c
												SET Active = ' + CONVERT(VARCHAR(1),@active) + '
												FROM
													' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.SecurityProject a INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.Process b ON
														    a.GroupID = @DG_id
														AND b.ProjectID = a.ProjectID
													INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessGroupSequence c ON
														c.ProcessID = b.ProcessID';
							
							EXEC(@sqlScript);
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'Error - Wrong Execution'
							SET @SQL      = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
				ELSE IF(@idType = 'PROJECT')
					BEGIN
						BEGIN TRY
							SET @sqlScript =   'UPDATE b
												SET Active = ' + CONVERT(VARCHAR(1),@active) + '
												FROM
													' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.Process a INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessGroupSequence b ON
														    a.ProjectID = ' + CONVERT(VARCHAR(10),@DG_id) + '
														AND b.ProcessID = a.ProcessID';							
							EXEC(@sqlScript);
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'Error - Wrong Execution'
							SET @SQL      = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
				ELSE IF(@idType = 'JOB')
					BEGIN
						BEGIN TRY
							SET @sqlScript =   'UPDATE ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessGroupSequence
												SET   Active = ' + CONVERT(VARCHAR(1),@active) + '
												WHERE ProcessGroupID = ' + CONVERT(VARCHAR(10),@DG_id);								
							EXEC(@sqlScript);
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'Error - Wrong Execution'
							SET @SQL      = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
				ELSE IF(@idType = 'TASK')
					BEGIN
						BEGIN TRY
							SET @sqlScript =   'UPDATE ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessGroupSequence
												SET   Active = ' + CONVERT(VARCHAR(1),@active) + '
												WHERE ProcessID = ' + CONVERT(VARCHAR(10),@DG_id);								
							EXEC(@sqlScript);
						END TRY
						BEGIN CATCH
							SET @continue = 0;
							SET @message  = 'Error - Wrong Execution'
							SET @SQL      = 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
						END CATCH
					END
				
				IF(@continue = 1)
					BEGIN
						COMMIT TRANSACTION
						SET @message = 'Success';
					END
				ELSE
					BEGIN
						ROLLBACK TRANSACTION
					END
			END
	
	SET @status = @continue;
END
GO
