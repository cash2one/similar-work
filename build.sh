#!/bin/bash

if [ ! -d output ]
then
    mkdir output
fi

rm -rf ./output/*

cp -r ./conf ./output
cp -r ./dao ./output
cp -r ./bll ./output
cp -r ./entrance ./output

find output -name '.svn' -exec rm -rf {} \;
find output -name '*.pyc' -exec rm -rf {} \;

mkdir ./output/log
mkdir ./output/data
VIDEO_TYPES=('movie' 'tv' 'comic' 'show')
DATA_TYPES=('source' 'intermediate' 'result')
for video_type in ${VIDEO_TYPES[@]}; do
    mkdir ./output/data/$video_type
    for data_type in ${DATA_TYPES[@]}; do
        mkdir ./output/data/$video_type/$data_type
    done
done

cp -r ./data/persistent ./output/data
