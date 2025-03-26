
echo $(date "+%Y-%m-%d %H:%M:%S")
git submodule update --init --remote --recursive

rm ./res/masterdb/data/*
rm -r ./res/masterdb/gakumasu-diff/json
rm -r ./res/masterdb/pretranslate_todo/

python3 main.py

cd output
git add --all
git commit -m "Update translate"
git push origin main