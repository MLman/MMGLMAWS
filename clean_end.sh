echo "Cleaning up temporary files..."
sleep 3
mkdir -p ~/junk
mv $tmp_dir/*.txt ../junk/
mv $tmp_dir/*.json ../junk/
