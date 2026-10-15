import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * Multi-threaded File Download Server (TCP)
 *
 * วิธีรัน: java FileServer <port> <shareDir> [io|nio] [virtual|N]
 * io  = Traditional I/O
 * nio = NIO ใช้ FileChannel.transferTo() ส่งข้อมูลไปยัง SocketChannel
 *
 * Protocol:
 * LIST               -> รายชื่อไฟล์
 * INFO <file>        -> ขนาดไฟล์
 * HASH <file>        -> ค่า SHA-256
 * GET <file> <off> <len> -> ดาวน์โหลดข้อมูลตามช่วงที่กำหนด
 * QUIT               -> จบการเชื่อมต่อ
 */
public class FileServer {

    static final int BUF_SIZE = 64 * 1024; // Buffer ขนาด 64 KiB
    static Path root;                      // โฟลเดอร์ที่อนุญาตให้แชร์ไฟล์
    static boolean nio;                    // เลือกโหมด NIO หรือ Traditional I/O

    public static void main(String[] args) throws Exception {

        // ตรวจสอบว่าผู้ใช้ระบุ Port และโฟลเดอร์ครบหรือไม่
        if (args.length < 2) {
            System.err.println("Usage: java FileServer <port> <shareDir> [io|nio] [virtual|N]");
            return;
        }

        int port = Integer.parseInt(args[0]);
        root = Path.of(args[1]).toAbsolutePath().normalize();
        nio = args.length > 2 && args[2].equalsIgnoreCase("nio");
        String threadOpt = args.length > 3 ? args[3] : "virtual";

        // เลือก Thread Pool สำหรับจัดการ Connection แต่ละรายการ
        ExecutorService pool = threadOpt.equalsIgnoreCase("virtual")
                ? Executors.newVirtualThreadPerTaskExecutor()
                : Executors.newFixedThreadPool(Integer.parseInt(threadOpt));

        System.out.printf("Server start port=%d dir=%s mode=%s threads=%s%n",
                port, root, nio ? "NIO(transferTo)" : "Traditional I/O", threadOpt);

        if (nio) {

            // เปิด ServerSocketChannel เพื่อรับ Connection แบบ NIO
            try (ServerSocketChannel ssc = ServerSocketChannel.open()) {
                ssc.bind(new InetSocketAddress(port));

                while (true) {
                    SocketChannel sc = ssc.accept(); // รอรับ Client

                    // ส่งงานของ Client รายนี้ไปให้ Thread Pool
                    pool.submit(() -> {
                        try {
                            return serve(sc.socket().getInputStream(),
                                    sc.socket().getOutputStream(), sc, sc);
                        } catch (IOException e) {
                            System.err.println("[conn] " + e);
                            try { sc.close(); } catch (IOException ignore) { }
                            return null;
                        }
                    });
                }
            }

        } else {

            // เปิด ServerSocket เพื่อรับ Connection แบบ Traditional I/O
            try (ServerSocket ss = new ServerSocket(port)) {

                while (true) {
                    Socket s = ss.accept(); // รอรับ Client

                    // ส่งงานของ Client รายนี้ไปให้ Thread Pool
                    pool.submit(() -> {
                        try {
                            return serve(s.getInputStream(),
                                    s.getOutputStream(), null, s);
                        } catch (IOException e) {
                            System.err.println("[conn] " + e);
                            try { s.close(); } catch (IOException ignore) { }
                            return null;
                        }
                    });
                }
            }
        }
    }

    // รับคำสั่งจาก Client แล้วเรียกเมธอดที่เกี่ยวข้อง
    static Void serve(InputStream in, OutputStream out,
                      SocketChannel ch, Closeable conn) {
        try (conn) {
            String line;

            // อ่านคำสั่งต่อไปจนกว่า Client จะปิด Connection
            while ((line = readLine(in)) != null) {
                String[] p = line.trim().split("\\s+");

                // แยกคำสั่งและส่งไปจัดการตามประเภท
                switch (p[0].toUpperCase()) {
                    case "LIST" -> list(out);
                    case "INFO" -> info(p, out);
                    case "HASH" -> hash(p, out);
                    case "GET"  -> get(p, in, out, ch);
                    case "QUIT" -> {
                        send(out, "BYE\n");
                        return null;
                    }
                    default -> send(out, "ERROR 400 Unknown command\n");
                }
            }
        } catch (IOException e) {
            System.err.println("[conn] " + e);
        }
        return null;
    }

    // ---------------- จัดการคำสั่งจาก Client ----------------

    // ส่งรายชื่อไฟล์และขนาดไฟล์ทั้งหมดในโฟลเดอร์ที่แชร์
    static void list(OutputStream out) throws IOException {
        StringBuilder sb = new StringBuilder();

        try (Stream<Path> s = Files.list(root)) {
            for (Path f : (Iterable<Path>) s.filter(Files::isRegularFile).sorted()::iterator) {
                String name = f.getFileName().toString();

                // ข้ามชื่อที่มีช่องว่าง เพราะ Protocol ใช้ Space แบ่งข้อมูล
                if (name.contains(" ")) continue;

                sb.append("FILE ").append(name).append(' ')
                        .append(Files.size(f)).append('\n');
            }
        }

        sb.append("END\n"); // แจ้งว่ารายชื่อไฟล์สิ้นสุดแล้ว
        send(out, sb.toString());
    }

    // ตรวจสอบไฟล์และส่งขนาดกลับไปยัง Client
    static void info(String[] p, OutputStream out) throws IOException {
        if (p.length != 2) {
            send(out, "ERROR 400 Usage: INFO <filename>\n");
            return;
        }

        Path f = resolve(p[1]);
        if (f == null) {
            send(out, "ERROR 404 File not found\n");
            return;
        }

        send(out, "SIZE " + Files.size(f) + "\n");
    }

    // คำนวณ SHA-256 แล้วส่งค่า Hash กลับไปยัง Client
    static void hash(String[] p, OutputStream out) throws IOException {
        if (p.length != 2) {
            send(out, "ERROR 400 Usage: HASH <filename>\n");
            return;
        }

        Path f = resolve(p[1]);
        if (f == null) {
            send(out, "ERROR 404 File not found\n");
            return;
        }

        send(out, "HASH " + sha256(f) + "\n");
    }

    // ตรวจสอบคำขอดาวน์โหลดก่อนส่งข้อมูลไฟล์
    static void get(String[] p, InputStream in,
                    OutputStream out, SocketChannel ch) throws IOException {

        // ตรวจสอบรูปแบบคำสั่ง GET
        if (p.length != 4) {
            send(out, "ERROR 400 Usage: GET <filename> <offset> <length>\n");
            return;
        }

        long off, len;

        // แปลง Offset และ Length ให้เป็นตัวเลข
        try {
            off = Long.parseLong(p[2]);
            len = Long.parseLong(p[3]);
        } catch (NumberFormatException e) {
            send(out, "ERROR 400 offset/length must be numbers\n");
            return;
        }

        Path f = resolve(p[1]);
        if (f == null) {
            send(out, "ERROR 404 File not found\n");
            return;
        }

        long size = Files.size(f);

        // ป้องกันการขอข้อมูลนอกขอบเขตไฟล์
        if (off < 0 || len < 0 || off > size || len > size - off) {
            send(out, "ERROR 416 Invalid range (file size=" + size + ")\n");
            return;
        }

        // ส่ง Header แจ้งจำนวนไบต์ที่จะส่งตามด้วยข้อมูลไฟล์จริง
        send(out, "OK " + len + "\n");

        // เลือกวิธีส่งข้อมูลตามโหมดที่กำหนด
        if (nio) {
            sendNio(f, off, len, ch);
        } else {
            sendTraditional(f, off, len, out);
        }
    }

    // ---------------- วิธีส่งข้อมูล 2 รูปแบบ ----------------

    // Traditional I/O: อ่านไฟล์เข้า Buffer แล้วส่งผ่าน OutputStream
    static void sendTraditional(Path f, long off, long len,
                                OutputStream out) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "r")) {

            raf.seek(off); // เลื่อนไปยังตำแหน่งเริ่มต้นที่ Client ขอ
            byte[] buf = new byte[BUF_SIZE];
            long remaining = len;

            // อ่านและส่งข้อมูลจนกว่าจะครบจำนวนที่ร้องขอ
            while (remaining > 0) {
                int n = raf.read(buf, 0, (int) Math.min(buf.length, remaining));

                if (n < 0) throw new EOFException("file shrank while sending");

                out.write(buf, 0, n);
                remaining -= n;
            }

            out.flush(); // ส่งข้อมูลที่ค้างอยู่
        }
    }

    // NIO: ใช้ transferTo ส่งข้อมูลจากไฟล์ไปยัง SocketChannel
    static void sendNio(Path f, long off, long len,
                        SocketChannel ch) throws IOException {
        try (FileChannel fc = FileChannel.open(f,
                java.nio.file.StandardOpenOption.READ)) {

            long pos = off, remaining = len;
            int zeros = 0;

            // วนส่งข้อมูลจนกว่าจะครบ แม้บางครั้งจะส่งได้ไม่หมดในครั้งเดียว
            while (remaining > 0) {
                long n = fc.transferTo(pos, remaining, ch);

                if (n == 0) {
                    if (pos >= fc.size())
                        throw new EOFException("file shrank while sending");

                    if (++zeros > 100000)
                        throw new IOException("transferTo stuck (buffer full)");

                    Thread.yield(); // เปิดโอกาสให้ Thread อื่นได้ทำงาน
                    continue;
                }

                zeros = 0; // รีเซ็ตตัวนับเมื่อส่งข้อมูลได้
                pos += n;
                remaining -= n;
            }
        }
    }

    // ---------------- เมธอดช่วยเหลือ ----------------

    // ตรวจสอบชื่อไฟล์และจำกัดการเข้าถึงให้อยู่ในโฟลเดอร์ที่แชร์
    static Path resolve(String name) {
        if (name.isEmpty() || name.contains("/") || name.contains("\\")
                || name.equals("..") || name.equals(".")) {
            return null;
        }

        Path f = root.resolve(name).normalize();

        return (f.getParent() != null
                && f.getParent().equals(root)
                && Files.isRegularFile(f)) ? f : null;
    }

    // ส่งข้อความ UTF-8 กลับไปยัง Client
    static void send(OutputStream out, String s) throws IOException {
        out.write(s.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    // อ่านข้อความจนพบ Newline โดยไม่อ่านล้ำไปถึงข้อมูลไฟล์
    static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        int b;

        while ((b = in.read()) != -1) {
            if (b == '\n') {
                return bo.toString(StandardCharsets.UTF_8).replace("\r", "");
            }

            // ป้องกันข้อความคำสั่งยาวผิดปกติ
            if (bo.size() > 8192) throw new IOException("line too long");

            bo.write(b);
        }

        return bo.size() == 0 ? null : bo.toString(StandardCharsets.UTF_8);
    }

    // คำนวณ SHA-256 ของไฟล์เพื่อใช้ตรวจสอบความถูกต้อง
    static String sha256(Path f) throws IOException {
        try (InputStream is = Files.newInputStream(f)) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[1 << 20]; // Buffer ขนาด 1 MiB
            int n;

            // อ่านไฟล์ทีละส่วนแล้วนำไปคำนวณ Hash
            while ((n = is.read(buf)) > 0) {
                md.update(buf, 0, n);
            }

            return HexFormat.of().formatHex(md.digest());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }
}