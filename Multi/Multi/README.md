### คอมไพล์ก่อน
javac -d classes FileServer.java FileClient.java

mkdir -p share สร้างโฟลเดอร์ share
dd if=/dev/urandom of=share/test.bin bs=1m count=512 สร้างไฟล์ทดสอบขนาด 512 MiB

### เปิด Server 
# java -cp classes FileServer <port> <dir> [io|nio] [virtual|N]
java -cp classes FileServer 8080 share io virtual
java -cp classes FileServer 8080 share nio virtual

### เปิด Client เพื่อทดสอบ อีก Terminal 
# java -cp classes FileClient <host> <port> download <file> [io|nio] [workers=10] [repeat=1] [outDir=downloads]
java -cp classes FileClient localhost 8080 download test.bin io 1 3 downloads
java -cp classes FileClient localhost 8080 download test.bin io 10 3 downloads

java -cp classes FileClient localhost 8080 download test.bin nio 1 3 downloads
java -cp classes FileClient localhost 8080 download test.bin nio 10 3 downloads

java -cp classes FileClient localhost 8080 download h2h.mp4 io 1 3 downloads
java -cp classes FileClient localhost 8080 download h2h.mp4 nio 10 1 downloads

java -cp classes FileClient localhost 8080 download h2h.mp4 nio 1 3 downloads
java -cp classes FileClient localhost 8080 download h2h.mp4 nio 10 3 downloads