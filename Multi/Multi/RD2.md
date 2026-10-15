### 1. คำสั่ง LIST (ดูรายชื่อไฟล์ทั้งหมดในโฟลเดอร์ share)
# java -cp classes FileClient <host> <port> list
java -cp classes FileClient localhost 8080 list

### 2. คำสั่ง INFO (ดูขนาดไฟล์)
# java -cp classes FileClient <host> <port> info <file>
java -cp classes FileClient localhost 8080 info test.bin
java -cp classes FileClient localhost 8080 info VDO.mov

### 3. คำสั่ง DOWNLOAD (ซึ่งเบื้องหลังจะยิงคำสั่ง INFO, HASH และ GET ตามช่วง Offset ให้เอง)
# java -cp classes FileClient <host> <port> download <file> [io|nio] [workers=10] [repeat=1] [outDir=downloads]
java -cp classes FileClient localhost 8080 download test.bin io 10 1 downloads