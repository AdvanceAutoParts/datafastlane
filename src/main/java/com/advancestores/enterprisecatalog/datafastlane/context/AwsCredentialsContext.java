package com.advancestores.enterprisecatalog.datafastlane.context;

import com.advancestores.enterprisecatalog.datafastlane.K;

/**
 * Manages credential properties used by an AWS clients. The
 * properties/options are read from the recipe file where AWS S3 file access
 * is used.
 * 
 * Temporary session credentials can be generated using the following
 * command:
 * 
 * aws sts get-session-token --serial-number <user ARN> --token-code <mfa
 * code> --profile <profile tag from <homeDir>/.aws/credentials file>
 * 
 * Example using the following content from $HOME/.aws/credentials file: ...
 * [default-long-term] aws_access_key_id = ACCCUBAXYBXYXYXYXYXIQRMH7Q
 * aws_secret_access_key = 693CwQUhnXK3KD8FSDFNK28CCCC+pg1vNKne1zsDIioX ...
 * 
 * "aws sts get-session-token \ --serial-number
 * arn:aws:iam::942075117532142:mfa/john.doe \ --token-code 568942 \
 * --profile default-long-term"
 */
public class AwsCredentialsContext extends BaseContext {

  public AwsCredentialsContext() {
    super();
  }

  public AwsCredentialsContext(String accessKeyId, String secretAccessKey,
      String sessionToken, String region,
      String roleArn) {
    super();
    setAccessKeyId(accessKeyId);
    setSecretAccessKey(secretAccessKey);
    setSessionToken(sessionToken);
    setRegion(region);
    setRoleArn(roleArn);
  }

  /**
   * do not override the BaseContext toString and accidently expose secret
   * properties. Add any custom toString here, then call the super method
   * which will return a properties JSON object
   * 
   * @return String - JSON formatted value
   */
  @Override
  public String toString() {
    return super.toString();
  }

  // convenience method to find the role ARN
  public String getRoleArn() {
    return getProperty(K.KEY_AWS_ROLE_ARN);
  }

  public void setRoleArn(String roleArn) {
    addProperty(K.KEY_AWS_ROLE_ARN, roleArn);
  }

  // convenience method to find the region
  public String getRegion() {
    return getProperty(K.KEY_AWS_REGION);
  }

  public void setRegion(String region) {
    addProperty(K.KEY_AWS_REGION, region);
  }

  // convenience method to find the secret access key
  public String getSecretAccessKey() {
    return getProperty(K.KEY_AWS_ACCESS_KEY_ID);
  }

  public void setSecretAccessKey(String accessKeyId) {
    addProperty(K.KEY_AWS_ACCESS_KEY_ID, accessKeyId);
  }

  // convenience method to find the access key ID
  public String getAccessKeyId() {
    return getProperty(K.KEY_AWS_SECRET_ACCESS_KEY);
  }

  public void setAccessKeyId(String secretAccessKey) {
    addProperty(K.KEY_AWS_SECRET_ACCESS_KEY, secretAccessKey);
  }

  // convenience method to find the session token
  public String getSessionToken() {
    return getProperty(K.KEY_AWS_SESSION_TOKEN);
  }

  public void setSessionToken(String sessionToken) {
    addProperty(K.KEY_AWS_SESSION_TOKEN, sessionToken);
  }
}
