## Premise: automation of rclone config
#!/bin/bash
# Create config from home directory
cd home
# Pull in web files
wget -O rclone-current-linux-amd64.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip
unzip rclone-current-linux-amd64.zip
#cp rclone-v*/rclone /usr/sbin; rm -rf rclone*
# Vuild working directory and perform install
mkdir ~/.config
mkdir ~/.config/rclone
cat > ~/.config/rclone/rclone.conf << EOF
[horelS3]
type = s3
env_auth = false
access_key_id = CCVF6SJ8KA9G69W439AI
secret_access_key = tmRYCMKhEeGAiIIE1LctLtAOroQuyXj5YM7UQh9u
#region = other-v2-signature
endpoint = https://pando-rgw01.chpc.utah.edu
location_constraint =

[AWS test]
type = s3
provider = AWS
env_auth = false
access_key_id = AKIATQDTW4IE46UH6NFD
secret_access_key = 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
region = us-west-2
EOF
# Load the module
module load rclone
