import java.io.*; // นำเข้าแพ็กเกจสำหรับจัดการ Input/Output ปกติ (InputStream, OutputStream, File ฯลฯ)
import java.net.*; // นำเข้าแพ็กเกจสำหรับระบบเครือข่าย (Socket, InetSocketAddress)
import java.nio.channels.*; // นำเข้าแพ็กเกจ NIO Channel สำหรับโอนถ่ายข้อมูลประสิทธิภาพสูง (FileChannel, SocketChannel)
import java.nio.charset.StandardCharsets; // นำเข้าคลาสกำหนดรหัสอักขระมาตรฐาน เช่น UTF-8
import java.nio.file.*; // นำเข้าแพ็กเกจจัดการระบบไฟล์ยุคใหม่ (Path, Files, StandardOpenOption)
import java.security.MessageDigest; // นำเข้าคลาสคำนวณ Hash (SHA-256) สำหรับตรวจเช็กความถูกต้องไฟล์
import java.util.*; // นำเข้าคลาสคอลเลกชันพื้นฐาน (List, ArrayList, HexFormat ฯลฯ)
import java.util.concurrent.*; // นำเข้าคลาสสำหรับการทำงานแบบขนาน Multi-threading (ExecutorService, Future)

/**
 * Multi-threaded File Download Client
 *
 * วิธีรัน:
 * java FileClient <host> <port> list
 * java FileClient <host> <port> info <file>
 * java FileClient <host> <port> download <file> [io|nio] [workers=10] [repeat=1] [outDir=downloads]
 *
 * แต่ละ Worker ใช้ Connection และตำแหน่งเขียนไฟล์ของตัวเอง
 * ทุก Worker เขียนลงไฟล์ปลายทางเดียวกันตาม Offset
 */
public class FileClient { 

    // Record เก็บข้อมูลขอบเขตงานของแต่ละ Worker: ID, จุดเริ่มเขียน (offset), และความยาวที่จะโหลด (length)
    record Range(int id, long offset, long length) {} // โครงสร้างข้อมูล Record แบบ Immutable

    public static void main(String[] a) throws Exception { 

        // ตรวจสอบว่าผู้ใช้ส่ง Argument มาเพียงพอหรือไม่ (อย่างน้อยต้องมี host, port, command)
        if (a.length < 3) { // ถ้าน้อยกว่า 3 ตัวแปร
            usage(); // เรียกแสดงวิธีใช้งาน
            return; // จบการทำงานทันที
        } 

        String host = a[0]; // ดึงข้อมูล Host/IP จาก Argument ตัวแรก
        int port = Integer.parseInt(a[1]); // แปลงportจากข้อความ Argument ตัวที่สองเป็นตัวเลข integer

        switch (a[2].toLowerCase()) { // ตรวจ Argument ตัวที่สาม โดยแปลงเป็นตัวพิมพ์เล็ก

            case "list" -> { // ดูรายชื่อไฟล์บน Server
                // เปิด Connection เชื่อมต่อไปยัง Server และปิดอัตโนมัติด้วย try-with-resources
                try (Conn c = new Conn(host, port)) { // สร้างวัตถุเชื่อมต่อ Socket
                    c.send("LIST"); // ส่งคำสั่ง "LIST\n" ไปหา Server

                    String line; // ตัวแปรเก็บข้อความแต่ละบรรทัดที่ตอบกลับ
                    while (!(line = c.readLine()).equals("END")) { // วนลูปอ่านบรรทัดถัดไปเรื่อยๆ จนกว่าจะเจอคำว่า "END"
                        System.out.println(line);
                    }
                }
            }

            case "info" -> { // ขอข้อมูลรายละเอียดไฟล์
                if (a.length < 4) { // ถ้าน้อยกว่า 4 แสดงว่าลืมระบุชื่อไฟล์
                    usage(); // แสดงวิธีใช้งาน
                    return; // จบการทำงาน
                }
                // เปิด Connection เพื่อขอข้อมูลขนาดไฟล์
                try (Conn c = new Conn(host, port)) { // เชื่อมต่อไปยัง Server
                    c.send("INFO " + a[3]); // ส่งคำสั่ง INFO พร้อมระบุชื่อไฟล์
                    System.out.println(c.readLine());
                }
            }

            case "download" -> { // เริ่มดาวน์โหลดไฟล์

                if (a.length < 4) { // ถ้าน้อยกว่า 4 ตัวแปร
                    usage(); // แสดงวิธีใช้งาน
                    return; // จบการทำงาน
                }

                String file = a[3]; // รับชื่อไฟล์ที่ต้องการดาวน์โหลด
                // ตรวจสอบโหมดรับข้อมูลว่าให้ใช้ "nio" หรือไม่ (ถ้าไม่ระบุหรือเป็นอย่างอื่นจะเป็น false คือใช้ Traditional IO)
                boolean nio = a.length > 4 // ตรวจสอบว่าส่ง Argument ตัวที่ 5 มาหรือไม่
                        && a[4].equalsIgnoreCase("nio"); // เช็กว่าเป็นคำว่า "nio" หรือไม่

                // กำหนดจำนวน Worker ถ้าระบุให้แปลงค่า ถ้าไม่ระบุให้ใช้ค่าเริ่มต้นเป็น 10
                int workers = a.length > 5 // ตรวจสอบ Argument ตัวที่ 6
                        ? Integer.parseInt(a[5]) : 10; // แปลงค่าเป็น integer หรือใช้ค่า 10

                // กำหนดจำนวนรอบที่จะทดสอบดาวน์โหลด ถ้าระบุให้แปลงค่า ถ้าไม่ระบุใช้ค่าเริ่มต้น 1 รอบ
                int repeat = a.length > 6 // ตรวจสอบ Argument ตัวที่ 7
                        ? Integer.parseInt(a[6]) : 1; // แปลงค่าเป็น integer หรือใช้ค่า 1

                // ตรวจสอบความถูกต้องของจำนวน Worker และรอบทดสอบ
                if (workers <= 0 || repeat <= 0) { // ต้องมีค่ามากกว่า 0 ทั้งคู่
                    System.out.println( // แสดงข้อความแจ้งเตือนข้อผิดพลาด
                            "workers and repeat must be greater than 0");
                    return;
                }

                // กำหนดโฟลเดอร์สำหรับเก็บไฟล์ปลายทาง (ถ้าไม่ระบุให้ใช้โฟลเดอร์ "downloads")
                Path outDir = Path.of( // สร้างวัตถุ Path
                        a.length > 7 ? a[7] : "downloads"); // ใช้ค่าจาก Argument ตัวที่ 8 หรือใช้ค่าเริ่มต้น "downloads"

                Files.createDirectories(outDir); // สร้างโฟลเดอร์ปลายทางและโฟลเดอร์ย่อยถ้ายังไม่มีอยู่จริง

                // เรียกเมธอดดาวน์โหลด วัดเวลา และทดสอบประสิทธิภาพ
                benchmark( // เริ่มกระบวนการทดสอบและดาวน์โหลด
                        host, port, file, nio, // โฮสต์, พอร์ต, ชื่อไฟล์, โหมด NIO
                        workers, repeat, outDir); // จำนวน worker, รอบทดสอบ, โฟลเดอร์เซฟ
            }

            default -> usage(); // กรณีระบุคำสั่งไม่ถูกต้อง ให้แสดงวิธีใช้งาน
        }
    }

    // // ฟังก์ชันแสดงคำแนะนำ Help
    static void usage() { 
        System.out.println("""
                Usage:
                java FileClient <host> <port> list
                java FileClient <host> <port> info <file>
                java FileClient <host> <port> download <file> [io|nio] [workers=10] [repeat=1] [outDir=downloads]
                """);
    }

    // เมธอดสำหรับดาวน์โหลดไฟล์ วัดเวลา คำนวณความเร็ว และเช็ก Hash ตรวจสอบความถูกต้อง
    static void benchmark( // เมธอดประมวลผลการดาวน์โหลดและเก็บสถิติ
            String host, int port, String file,
            boolean nio, int workers, int repeat,
            Path outDir) throws Exception {

        long size; // ขนาดไฟล์ทั้งหมด (Bytes)
        String expectedHash; // เก็บค่า SHA-256 Hash จาก Server ใช้ตรวจความถูกต้อง

        // ติดต่อ Server เพื่อขอขนาดไฟล์และค่า Hash ก่อนเริ่มดาวน์โหลดจริง
        try (Conn c = new Conn(host, port)) { // เปิด Connection ชั่วคราว

            c.send("INFO " + file); // ส่งคำสั่งขอขนาดไฟล์
            String response = c.readLine(); // รับข้อความตอบกลับจาก Server

            if (response == null || !response.startsWith("SIZE ")) { // ถ้า Server ไม่ได้ตอบกลับด้วย "SIZE "
                throw new IOException( // แจ้งข้อผิดพลาด
                        "Server replied: " + response);
            }

            size = Long.parseLong(response.substring(5)); // แปลงข้อความตัวเลขขนาดไฟล์หลัง "SIZE " ให้เป็น long

            c.send("HASH " + file); // ส่งคำสั่งขอค่า SHA-256 Hash
            response = c.readLine(); // รับข้อความตอบกลับ

            if (response == null // ถ้าไม่มีข้อมูลตอบกลับ
                    || !response.startsWith("HASH ")) { // หรือคำตอบไม่ได้ขึ้นต้นด้วย "HASH "
                throw new IOException( // แจ้งข้อผิดพลาด
                        "Server replied: " + response);
            }

            expectedHash = response.substring(5); // ดึงข้อความ Hash จริงหลังคำว่า "HASH "
        }

        String mode = nio ? "nio" : "io"; // แปลงค่าสถานะ boolean เป็นข้อความชื่อโหมด ("nio" หรือ "io")

        // พิมพ์ข้อมูลสรุปรายละเอียดไฟล์และการตั้งค่าคอนฟิกก่อนเริ่มทำงาน
        System.out.printf( 
                "File=%s size=%d bytes (%.2f MB) mode=%s workers=%d repeat=%d%n",
                file, size, size / 1_048_576.0, // ชื่อไฟล์, ขนาด byte, ขนาด MB
                mode, workers, repeat); // โหมด I/O, จำนวน worker, จำนวนรอบ

        // คำนวณแบ่งช่วงไฟล์ออกเป็นส่วนๆ ให้กับแต่ละ Worker
        List<Range> ranges = computeRanges(size, workers); // คำนวณขอบเขตข้อมูล
        for (Range r : ranges) { // วนลูปแสดงขอบเขตงานของแต่ละ Worker
            System.out.printf(
                    "  worker %2d : offset=%d length=%d%n",
                    r.id(), r.offset(), r.length()); // ID ของ Worker, จุดเริ่มเขียน, ความยาว
        } // จบการลูปแสดงช่วงข้อมูล

        double[] secs = new double[repeat]; // เก็บเวลาที่ใช้ดาวน์โหลดในแต่ละรอบ (หน่วยวินาที)
        for (int run = 1; run <= repeat; run++) {
            Path out = outDir.resolve(file); // สร้างพาธไฟล์เต็มที่จะเขียนลงดิสก์
            // ลบไฟล์เดิมทิ้งก่อนเริ่มดาวน์โหลดรอบใหม่ เพื่อป้องกันข้อมูลตกค้าง
            Files.deleteIfExists(out); // ลบไฟล์ปลายทางหากมีอยู่แล้ว
            long t0 = System.nanoTime(); // บันทึกเวลาเริ่มต้นดาวน์โหลด (nanoseconds)

            // เริ่มกระบวนการดาวน์โหลดไฟล์ 1 รอบ โดยกระจายงานให้หลาย Worker
            downloadOnce(
                    host, port, file,
                    size, ranges, nio, out);

            // คำนวณเวลาที่ใช้ไปทั้งหมดในรอบนี้ (แปลง nanoseconds เป็นวินาที)
            double sec = // คำนวณระยะเวลา
                    (System.nanoTime() - t0) / 1e9; // เวลาสิ้นสุด - เวลาเริ่มต้น หารด้วย 10^9

            // ตรวจสอบความถูกต้อง: เช็กขนาดไฟล์จริงบนดิสก์ และคำนวณ Hash เทียบกับ Server
            boolean sizeOk = Files.size(out) == size; // เช็กขนาดไฟล์บนดิสก์ว่าเท่ากับขนาดจาก Server หรือไม่
            boolean hashOk = sha256(out).equals(expectedHash); // เช็กว่าค่า SHA-256 ตรงกับค่าที่ Server แจ้งหรือไม่

            // คำนวณอัตราความเร็วการดาวน์โหลด (MB/s)
            double mbps = // คำนวณ Throughput
                    size / 1_048_576.0 / sec; // ขนาด MB หารด้วยเวลาในหน่วยวินาที

            secs[run - 1] = sec; // บันทึกเวลาของรอบปัจจุบันลงในอาร์เรย์
            // คำนวณ Hash จริงของไฟล์ในเครื่องเพื่อนำมาแสดงผล
            String actualHash = sha256(out);

            System.out.printf(
                    "run %d: time=%.3f s throughput=%.2f MB/s size=%d bytes (%s) sha256=%s (%s)%n",
                    run, sec, mbps,
                    Files.size(out), sizeOk ? "OK" : "MISMATCH",
                    actualHash, hashOk ? "OK" : "MISMATCH");

            // พิมพ์ผลลัพธ์ในรูปแบบ CSV เพื่อนำไปวิเคราะห์ข้อมูลต่อได้ง่าย
            System.out.printf( // พิมพ์บรรทัด CSV
                    "RESULT,%s,%d,%d,%.3f,%.2f,%s%n", // โครงสร้าง CSV
                    mode, workers, run, sec, mbps, // โหมด, จำนวน worker, รอบที่, เวลา, ความเร็ว
                    (sizeOk && hashOk) ? "OK" : "FAIL"); // สรุปความถูกต้องภาพรวม
        } 

        // คำนวณค่าเฉลี่ยและสรุปผลการทดสอบรวมทุกรอบ
        double avg = // คำนวณเวลาเฉลี่ย
                Arrays.stream(secs).average().orElse(0); // หาค่าเฉลี่ยจากอาร์เรย์เก็บเวลา

        double avgMbps = avg > 0 // คำนวณความเร็วเฉลี่ย MB/s
                ? size / 1_048_576.0 / avg : 0; // ถ้าเวลาเฉลี่ยมากกว่า 0 ให้คำนวณความเร็ว

        System.out.printf(
                "SUMMARY mode=%s workers=%d avg=%.3f s (%.2f MB/s) min=%.3f s max=%.3f s%n",
                mode, workers, avg, avgMbps, // โหมด, จำนวน worker, เวลาเฉลี่ย, ความเร็วเฉลี่ย
                Arrays.stream(secs).min().orElse(0), // เวลาที่ใช้รวดเร็วที่สุด
                Arrays.stream(secs).max().orElse(0)); // เวลาที่ใช้ช้าที่สุด
    }

    // เมธอดสำหรับคำนวณแบ่งขนาดไฟล์ออกเป็นช่วงๆ (Ranges) ที่ไม่ซ้อนทับกัน
    static List<Range> computeRanges(long size, int n) { 
        List<Range> list = new ArrayList<>(); // สร้าง List สำหรับเก็บวัตถุ Range ของ Worker ทั้งหมด
        long chunk = size / n; // คำนวณขนาดบล็อกข้อมูลพื้นฐานสำหรับแต่ละ Worker

        for (int i = 0; i < n; i++) { // วนลูปสร้าง Range ตามจำนวน Worker (n)
            long offset = i * chunk; // คำนวณตำแหน่งเริ่มต้น (offset) ของ Worker ตัวนี้

            // คำนวณความยาวข้อมูล: ถ้าเป็น Worker ตัวสุดท้าย ให้เก็บส่วนที่เหลือทั้งหมดจนจบไฟล์
            long length = (i == n - 1) // ตรวจสอบว่าเป็น Worker ตัวสุดท้ายหรือไม่
                    ? size - offset : chunk; // ถ้าใช่ให้เก็บขนาดที่เหลือทั้งหมด ถ้าไม่ใช่ใช้ขนาด chunk ปกติ

            list.add(new Range(i, offset, length)); // เพิ่มวัตถุ Range เข้าไปใน List
        }

        return list;
    }

    // เมธอดสำหรับดาวน์โหลดไฟล์ 1 รอบ โดยสร้างไฟล์เปล่าเตรียมไว้และรัน Worker พร้อมกันหลาย Thread
    static void downloadOnce(
            String host, int port, String file,
            long size, List<Range> ranges,
            boolean nio, Path out) throws Exception {

        // สร้างไฟล์ใหม่และจองพื้นที่ล่วงหน้าบนดิสก์ตามขนาดจริง
        try (FileChannel fc = FileChannel.open( // เปิด FileChannel สำหรับไฟล์ปลายทาง
                out,
                StandardOpenOption.CREATE_NEW, // สร้างไฟล์ใหม่เท่านั้น (ถ้ามีไฟล์อยู่แล้วจะ throw exception)
                StandardOpenOption.WRITE, // เปิดในโหมดเขียน
                StandardOpenOption.SPARSE)) { // เปิดโหมด Sparse File เพื่อประหยัดการเขียนดิสก์จริงขณะจองพื้นที่

            if (size > 0) { // ถ้าขนาดไฟล์มากกว่า 0 bytes
                fc.write( // เขียน byte หลอก 1 ตัวลงไปที่ตำแหน่งท้ายสุด เพื่อจองขนาดไฟล์ไว้ทันที
                        java.nio.ByteBuffer.wrap(new byte[1]), // บัฟเฟอร์ byte ข้อมูลหลอก 1 byte
                        size - 1); // เลื่อนไปเขียนที่ตำแหน่งสุดท้ายของไฟล์ (size - 1)
            }
        }

        // สร้าง Thread Pool ที่มีจำนวน Thread ฟิกซ์ตามจำนวน Worker (ranges.size())
        ExecutorService pool = // สร้างวัตถุจัดการ Thread
                Executors.newFixedThreadPool(ranges.size()); // สร้าง Fixed Thread Pool

        try { // เริ่มต้นการรันงานแบบ Multi-threading
            List<Future<Long>> futures = new ArrayList<>(); // รายการเก็บสถานะ Future ของแต่ละ Worker

            // วนลูปส่ง Task ของ Worker แต่ละตัวเข้าไปทำงานใน Thread Pool
            for (Range r : ranges) { 

                futures.add(pool.submit(() -> // ส่ง Task เข้า Thread Pool และเก็บวัตถุ Future ไว้
                        nio // ตรวจสอบว่าใช้โหมด NIO หรือไม่
                                ? workerNio( // ถ้าใช้ NIO ให้เรียกเมธอด workerNio
                                        host, port, file, r, out) // พารามิเตอร์งาน NIO
                                : workerTraditional( // ถ้าไม่ใช่ ให้เรียกเมธอด workerTraditional
                                        host, port, file, r, out))); // พารามิเตอร์งาน Traditional IO
            }

            long total = 0; // ตัวแปรรวมจำนวน Bytes ทั้งหมดที่ Worker ทุกตัวรับมาได้
            for (Future<Long> future : futures) { // วนลูปตรวจสอบผลงานของแต่ละ Thread

                try { // ลองดึงผลลัพธ์จาก Future
                    total += future.get(); // รอให้ Thread ทำงานเสร็จและนำจำนวน byte ที่ดาวน์โหลดได้มารวมกัน

                } catch (ExecutionException e) { // ถ้าระหว่างทำงานใน Thread เกิด Exception ขึ้น
                    throw new IOException( // ห่อ Exception และ throw ออกไปแจ้งว่า Worker ทำงานล้มเหลว
                            "worker failed: " + e.getCause(), // ข้อความแจ้งเตือน
                            e.getCause()); // ต้นเหตุ Exception
                }
            }

            // ตรวจสอบรวมจำนวน bytes ที่ได้ว่าเท่ากับขนาดไฟล์จริงหรือไม่
            if (total != size) { 
                throw new IOException( // แจ้งข้อผิดพลาดขนาดไฟล์ไม่ครบถ้วน
                        "received " + total // จำนวนที่รับได้จริง
                                + " bytes, expected " + size); // จำนวนที่คาดหวัง
            } 

        } finally { // บล็อกที่ทำงานเสมอเพื่อคืนทรัพยากร
            pool.shutdownNow(); // สั่งยกเลิกงานและปิด Thread Pool
        }
    }

    // ***************************** เมธอดสำหรับดาวน์โหลดช่วงข้อมูลด้วย Traditional I/O **************************//
    static long workerTraditional( 
            String host, int port, String file,
            Range r, Path out) throws IOException {

        if (r.length() == 0) { // ถ้าช่วงข้อมูลยาว 0 bytes
            return 0; // คืนค่า 0 ทันที ไม่ต้องเปิดการเชื่อมต่อ
        }

        // แต่ละ Worker สร้าง Connection Socket และ File Handle (RandomAccessFile) เป็นของตัวเอง
        try (Socket socket = new Socket(host, port); // เปิด Socket เชื่อมต่อไปยัง Server
             RandomAccessFile raf = // เปิดไฟล์ปลายทางเพื่อเขียนข้อมูลตามตำแหน่ง Offset
                     new RandomAccessFile(out.toFile(), "rw")) { // โหมดอ่าน-เขียน ("rw")

            InputStream in = new BufferedInputStream( // ห่อ InputStream ของ Socket ด้วย BufferedInputStream
                    socket.getInputStream(), 64 * 1024); // บัฟเฟอร์ขนาด 64KB ช่วยเพิ่มประสิทธิภาพอ่านเครือข่าย

            OutputStream os = socket.getOutputStream(); // ดึง OutputStream ของ Socket สำหรับส่งคำสั่ง

            // ส่งคำสั่ง GET ระบุช่วง offset และ length ที่ Worker ตัวนี้รับผิดชอบ
            os.write(( // ส่งชุดคำสั่งผ่าน Socket
                    "GET " + file + " " // คำสั่ง GET ตามด้วยชื่อไฟล์
                            + r.offset() + " " // จุดเริ่มต้น Offset
                            + r.length() + "\n") // ความยาวไบต์ที่ต้องการและขึ้นบรรทัดใหม่
                    .getBytes(StandardCharsets.UTF_8)); // แปลงข้อความสั่งเป็น Byte UTF-8

            os.flush(); // ดันข้อมูลสั่งการออกไปหา Server ทันที

            // อ่าน Header ที่ตอบกลับจาก Server และตรวจสอบความถูกต้อง
            long len = parseOk(readLine(in), r); // แปลงความยาวข้อมูลที่ตอบกลับมา

            // เลื่อนตำแหน่งเขียนไฟล์ไปยัง Offset ของ Worker ตัวนี้
            raf.seek(r.offset()); // เลื่อน pointer การเขียนไฟล์ไปที่ตำแหน่ง r.offset()

            byte[] buffer = new byte[64 * 1024]; // บัฟเฟอร์ขนาด 64KB สำหรับรับข้อมูลในแต่ละรอบ
            long remaining = len; // จำนวน bytes ที่ยังคงต้องอ่านให้ครบถ้วน

            // วนลูปอ่านข้อมูลจาก Network แล้วเขียนลงดิสก์จนกว่าจะครบช่วงที่รับผิดชอบ
            while (remaining > 0) { // วนลูปอ่านข้อมูลตราบใดที่ยังเหลือ bytes ต้องอ่าน

                int n = in.read( // อ่านข้อมูลจาก InputStream เข้าบัฟเฟอร์
                        buffer, // อาร์เรย์บัฟเฟอร์
                        0, // ตำแหน่งเริ่มเก็บในอาร์เรย์
                        (int) Math.min( // อ่านเท่าขนาดบัฟเฟอร์ หรือเท่าจำนวนที่เหลือถ้าเหลือน้อยกว่า
                                buffer.length, remaining)); // เลือกค่าที่น้อยกว่า

                if (n < 0) { // ถ้าอ่านได้ค่าติดลบ แสดงว่า Server ปิด Connection ก่อนส่งครบ
                    throw new EOFException( // แจ้งการหลุดการเชื่อมต่อก่อนกำหนด
                            "connection closed early (worker " // ข้อความแจ้งเตือน
                                    + r.id() + ")"); // ระบุ ID ของ Worker
                } 

                raf.write(buffer, 0, n); // เขียนข้อมูลจากบัฟเฟอร์จำนวน n bytes ลงในไฟล์ที่ Offset ปัจจุบัน
                remaining -= n; // หักลบจำนวน bytes ที่อ่านเขียนได้สำเร็จออกจาก remaining
            }

            return len; // คืนค่าจำนวน bytes ที่ดาวน์โหลดสำเร็จ
        } // ปิด Socket และ RandomAccessFile อัตโนมัติเมื่อออกจาก try
    }

    // ******************************* เมธอดสำหรับดาวน์โหลดช่วงข้อมูลด้วย Java NIO (SocketChannel / FileChannel / zero-copy transfer)
    static long workerNio( // เมธอด Worker แบบ NIO
            String host, int port, String file, // ข้อมูลระบบและชื่อไฟล์
            Range r, Path out) throws IOException { // ช่วงข้อมูลและพาธไฟล์ปลายทาง

        if (r.length() == 0) { // ถ้าความยาวช่วงข้อมูลเป็น 0
            return 0; // คืนค่า 0 ทันที
        }

        // เปิด Network Channel (SocketChannel) และ File Channel (FileChannel)
        try (SocketChannel sc = SocketChannel.open( // เปิด SocketChannel
                    new InetSocketAddress(host, port)); // เชื่อมต่อไปยัง Server แบบ Non-blocking/NIO
             FileChannel fc = FileChannel.open( // เปิด FileChannel บนไฟล์ปลายทาง
                    out, StandardOpenOption.WRITE)) { // กำหนดโหมดเขียนไฟล์

            OutputStream os = // ดึง OutputStream จาก Socket ของ SocketChannel
                    sc.socket().getOutputStream(); // ดึง Stream เพื่อส่งคำสั่ง

            // ส่งคำสั่ง GET ระบุช่วง offset และ length ที่ต้องการ
            os.write(( // ส่งคำสั่งผ่าน Stream
                    "GET " + file + " " // คำสั่ง GET และชื่อไฟล์
                            + r.offset() + " " // จุดเริ่มต้น Offset
                            + r.length() + "\n") // ความยาวและขึ้นบรรทัดใหม่
                    .getBytes(StandardCharsets.UTF_8)); // แปลงเป็น UTF-8

            os.flush(); // ดันข้อมูลออกทันที

            // อ่าน Header จาก Server เพื่อรับทราบขนาดข้อมูลก่อนเริ่มรับไฟล์
            long len = parseOk( // ตรวจสอบและแปลง Header ตอบกลับ
                    readLine(sc.socket().getInputStream()), r); // อ่านบรรทัด Header แรก

            long position = r.offset(); // กำหนดตำแหน่งเริ่มต้นในการเขียนบน FileChannel
            long remaining = len; // จำนวน bytes ที่เหลือต้องโอนย้าย

            // ************************************** วนลูปรับข้อมูลเครือข่ายเขียนตรงลงดิสก์ด้วย transferFrom (Zero-copy optimization)
            while (remaining > 0) { // วนลูปโอนย้ายข้อมูลจนครบช่วง

                long n = fc.transferFrom( // โอนย้ายข้อมูลตรงจาก SocketChannel เข้า FileChannel
                        sc, position, remaining); // แหล่งข้อมูล, ตำแหน่งเขียนไฟล์, จำนวนที่ต้องการ

                if (n <= 0) { // ถ้าไม่มีการโอนย้ายข้อมูลเกิดขึ้น (หลุดการเชื่อมต่อ)
                    throw new EOFException( // แจ้งข้อผิดพลาด Connection ปิดก่อนกำหนด
                            "connection closed early (worker " // ข้อความแจ้งเตือน
                                    + r.id() + ")"); // ระบุ ID ของ Worker
                }

                position += n; // ขยับตำแหน่งเขียนไฟล์เพิ่มขึ้นตามจำนวน bytes ที่โอนย้ายสำเร็จ
                remaining -= n; // หักลบจำนวน bytes ที่โอนย้ายได้สำเร็จออกจาก remaining
            }

            return len; // คืนค่าจำนวน bytes ที่ดาวน์โหลดสำเร็จ
        }
    }

    // เมธอดสำหรับตรวจสอบ Header และรับขนาดจำนวนไบต์ที่ Server ตอบกลับมา
    static long parseOk( // เมธอดแกะและเช็กข้อความ Header ตอบกลับ
            String header, Range r) throws IOException { // ข้อความ Header และวัตถุ Range ของ Worker

        if (header == null) { // ถ้าไม่ได้ข้อความตอบกลับจาก Server
            throw new EOFException("no response"); // แจ้งไม่มีการตอบกลับ
        } // จบเช็ก null

        if (header.startsWith("ERROR")) { // ถ้า Server ตอบกลับข้อความ error
            throw new IOException( // แจ้งข้อผิดพลาดจาก Server
                    "worker " + r.id() + ": " + header); // ข้อความ error พร้อม ID
        } // จบเช็ก ERROR

        if (!header.startsWith("OK ")) { // ถ้าตอบกลับรูปแบบอื่นที่ไม่ใช่ขึ้นต้นด้วย "OK "
            throw new IOException( // แจ้งรูปแบบตอบกลับไม่ถูกต้อง
                    "bad response: " + header); // แสดงข้อความที่ไม่ถูกต้อง
        } // จบเช็กรูปแบบ OK

        long len = Long.parseLong( // ตัดข้อความหลังคำว่า "OK " แล้วแปลงเป็นตัวเลข long
                header.substring(3).trim()); // ดึงตัวเลขและตัดช่องว่างทิ้ง

        // ตรวจสอบขนาดข้อมูลที่ Server จะส่งมาว่าตรงกับขนาดช่วงที่ขอไปหรือไม่
        if (len != r.length()) { // ถ้าขนาดไม่ตรงกัน
            throw new IOException( // แจ้งข้อผิดพลาดขนาดข้อมูลไม่ตรงกัน
                    "length mismatch: " // ข้อความแจ้งเตือน
                            + len + " != " + r.length()); // แสดงขนาดจริง เทียบกับ ขนาดที่คาดหวัง
        } // จบการเช็กความยาวข้อมูล

        return len; // คืนค่าความยาวข้อมูลที่ถูกต้อง
    } // จบเมธอด parseOk

    // เมธอดช่วยสำหรับอ่านข้อความทีละอักขระจาก InputStream จนกว่าจะพบ Newline ('\n')
    static String readLine(InputStream in) // เมธอดอ่านข้อความ 1 บรรทัด
            throws IOException { // การจัดการ Exception

        ByteArrayOutputStream bo = // สร้าง Stream สำหรับสะสมไบต์อักขระ
                new ByteArrayOutputStream(); // บัฟเฟอร์เก็บไบต์ในหน่วยความจำ

        int b; // ตัวแปรเก็บค่าไบต์ที่อ่านได้

        while ((b = in.read()) != -1) { // วนลูปอ่านไบต์ทีละตัวจาก InputStream จนกว่าจะหมดข้อมูล (-1)

            if (b == '\n') { // ถ้าเจอตัวอักษรขึ้นบรรทัดใหม่ ('\n')
                return bo.toString( // แปลงไบต์ทั้งหมดที่สะสมไว้เป็น String รหัส UTF-8
                        StandardCharsets.UTF_8) // รหัส UTF-8
                        .replace("\r", ""); // ลบตัวอักษร Carriage Return ('\r') ออกถ้ามี
            }

            bo.write(b); // เพิ่มไบต์ที่อ่านได้ลงในสะสมไบต์
        }

        return bo.size() == 0 // ถ้าอ่านจนจบ Stream (-1) แล้วไม่มีข้อมูลสะสมเลย
                ? null // คืนค่า null
                : bo.toString(StandardCharsets.UTF_8); // ถ้ามีข้อมูลสะสม คืนค่าข้อความทั้งหมด
    }

    // เมธอดสำหรับคำนวณค่า SHA-256 Hash ของไฟล์เพื่อใช้ยืนยันความถูกต้องครบถ้วน
    static String sha256(Path file) throws IOException {

        try (InputStream in = // เปิด InputStream สำหรับอ่านไฟล์ดิสก์
                     Files.newInputStream(file)) { // อ่านจากพาธไฟล์

            MessageDigest md = // สร้างวัตถุสำหรับคำนวณ Hash
                    MessageDigest.getInstance("SHA-256"); // กำหนดอัลกอริทึม SHA-256

            byte[] buffer = new byte[1 << 20]; // บัฟเฟอร์ขนาด 1MB (1024x1024 bytes)
            int n; // เก็บจำนวนไบต์ที่อ่านได้ในแต่ละรอบ

            // อ่านไฟล์ทีละส่วนเพื่อทยอยอัปเดตข้อมูลเข้าตัวคำนวณ Hash
            while ((n = in.read(buffer)) > 0) { // อ่านจนกว่าจะหมดไฟล์ (n <= 0)
                md.update(buffer, 0, n); // ป้อนไบต์ข้อมูลจำนวน n เข้าสู่กระบวนการคำนวณ Hash
            }

            // คำนวณ Hash สรุปผลลัพธ์และแปลงเป็น String เลขฐาน 16
            return HexFormat.of().formatHex(md.digest()); // แปลงอาร์เรย์ไบต์ผลลัพธ์เป็น Hex String

        } catch (java.security.NoSuchAlgorithmException e) { // ถ้าระบบไม่รองรับอัลกอริทึม SHA-256
            throw new IOException(e); // ห่อ Exception และ throw ออกไป
        }
    }

    // คลาสภายในช่วยจัดการ Connection Socket พื้นฐานสำหรับคำสั่ง LIST, INFO และ HASH
    static class Conn implements Closeable { // คลาสจัดการ Socket Connection

        final Socket s; // วัตถุ Socket เชื่อมต่อ
        final InputStream in; // Stream อ่านข้อมูลเข้า
        final OutputStream out; // Stream ส่งข้อมูลออก

        Conn(String host, int port) throws IOException { // คอนสตรักเตอร์เปิดการเชื่อมต่อ

            // สร้าง Socket และเชื่อมต่อไปยังเซิร์ฟเวอร์ปลายทาง
            s = new Socket(host, port); // สร้างการเชื่อมต่อ TCP

            in = s.getInputStream(); // ดึง Stream ขาเข้า
            out = s.getOutputStream(); // ดึง Stream ขาออก
        } // จบ คอนสตรักเตอร์ Conn

        // เมธอดส่งคำสั่งข้อความไปยัง Server
        void send(String cmd) throws IOException { // เมธอดส่งข้อความคำสั่ง

            out.write( // เขียนข้อความลง Stream ขาออก
                    (cmd + "\n") // เติม character ขึ้นบรรทัดใหม่ ต่อท้ายคำสั่ง
                            .getBytes(StandardCharsets.UTF_8)); // แปลงเป็นอาร์เรย์ไบต์ UTF-8

            out.flush(); // ดันข้อมูลทั้งหมดในบัฟเฟอร์ออกไปทาง Socket ทันที
        }

        // เมธอดอ่านคำตอบข้อความ 1 บรรทัดจาก Server
        String readLine() throws IOException { // เมธอดอ่านข้อความตอบกลับ
            return FileClient.readLine(in); // เรียกใช้เมธอดช่วยเหลือ readLine หลักของ FileClient
        }

        // เมธอดสำหรับปิด Connection และคืนทรัพยากร Socket
        @Override
        public void close() throws IOException { // เมธอดตามอินเทอร์เฟซ Closeable
            s.close(); // ปิดการทำงานของ Socket
        }
    }
}