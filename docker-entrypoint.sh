eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa

git config --global user.email "$GIT_EMAIL"
git config --global user.name "$GIT_NAME"

git pull origin main

git submodule update --init --remote --recursive

python3 main.py

cd output
git add --all
git commit -m "Update translate"
git push origin main
bash