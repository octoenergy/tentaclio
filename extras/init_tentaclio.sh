#!/usr/bin/env bash

# Init secrets file
secrets_file="$HOME/.tentaclio.yml"
echo "🐙 Creating tentaclio's secrets file if not present..."
touch $secrets_file
if ! [[ -s $secrets_file ]]; then
    cat > $secrets_file <<- EOF
secrets:
    consumer_db: postgresql://USER:PASSWORD@DATABASE-HOST-NAME/consumer
EOF
fi

# Add secrets file to profile file.
if [ -z "$shell" ]; then
  shell="$(ps c -p "$PPID" -o 'ucomm=' 2>/dev/null || true)"
  shell="${shell##-}"
  shell="${shell%% *}"
  shell="$(basename "${shell:-$SHELL}")"
fi

echo "📃 Adding TENTACLIO__SECRETS_FILE to profile file."
case "$shell" in
    bash )
        profile="$HOME/.bashrc"
        ;;
    zsh )
        profile="$HOME/.zshrc"
        ;;
    ksh )
        profile="$HOME/.profile"
        ;;
    fish )
        profile="$HOME/.config/fish/config.fish"
        ;;
    * )
        echo "🤔 I don't know how to cofigure your $shell, please add"
        echo ""
        echo "\texport TENTACLIO__SECRETS_FILE=${HOME}/.tentaclio.yml"
        echo ""
        echo "to your profile file."
        return
        ;;
esac

if [[ -z $(grep TENTACLIO__SECRETS_FILE $profile) ]]; then
    echo "export TENTACLIO__SECRETS_FILE=$HOME/.tentaclio.yml # tentaclio secrets file" >> $profile
else
    echo "🙅 Envrionmental variable already in profile file, doing nothing."
fi

echo "🕵️  Now you can edit ~/.tentaclio.yml to add your secrets"
