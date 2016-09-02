docker run --name companion_rf --link companion_db:companion_db -v /Volumes/CompanionEx/Debug/hdf:/root/hdf -v /Volumes/CompanionEx/Data/ndw/:/root/ndw -v /Volumes/CompanionEx/Debug/kml/:/root/kml companion_rf:latest > /dev/null &

