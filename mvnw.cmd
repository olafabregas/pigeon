@ECHO OFF
SETLOCAL
SET BASE_DIR=%~dp0
SET BASE_DIR=%BASE_DIR:~0,-1%
IF DEFINED JAVA_HOME (
  SET JAVA_EXEC=%JAVA_HOME%\bin\java.exe
) ELSE (
  SET JAVA_EXEC=java.exe
)
"%JAVA_EXEC%" "-Dmaven.multiModuleProjectDirectory=%BASE_DIR%" -classpath "%BASE_DIR%\.mvn\wrapper\maven-wrapper.jar" org.apache.maven.wrapper.MavenWrapperMain %*
ENDLOCAL
