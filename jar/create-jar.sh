#!/bin/sh

# Création d'archive exécutable jar

recur_path() {
  for arg in $(ls "$1"); do
    if test -d "$1/$arg"; then
	recur_path "$1/$arg"
    else echo "$1/$arg"
    fi
  done
}

if test "$#" -ge 1; then
  dest=$(echo "$1" | sed 's/\.java$//g')
  echo "Main-Class: $dest" > MANIFEST.MF
  mkdir tmp_src
  javac $* -d tmp_src # Compilation
  if test $? -eq 0; then
    args=''
    for arg in $(recur_path tmp_src); do
	file_name=$(echo $arg | sed "s/^tmp_src\///")
	args="$args -C tmp_src $file_name"
    done
    jar cmf MANIFEST.MF "$dest".jar $args # Création de l'archive
  fi
  rm -R MANIFEST.MF tmp_src
else echo "Usage: mjar MainClass UsefullClasses..."
fi
