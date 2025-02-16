git pull origin main

git submodule update --init --remote --recursive

python3 main.py

cd output
git add --all
git commit -m "Update translate"
git push origin main