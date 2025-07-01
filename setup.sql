// Step1: テーブル作成 //
-- ロールの指定
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

// Step2: 各種オブジェクトの作成 //
CREATE OR REPLACE DATABASE citibike_qq;

// Step3: 公開されているGitからデータとスクリプトを取得 //
-- Git連携のため、API統合を作成する
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-mmatsushita')
  ENABLED = TRUE;

-- GIT統合の作成
CREATE OR REPLACE GIT REPOSITORY GIT_INTEGRATION_FOR_HANDSON
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/sfc-gh-mmatsushita/qq_zero2snowflake_cb.git';
  
-- チェックする
ls @GIT_INTEGRATION_FOR_HANDSON/branches/main/;
