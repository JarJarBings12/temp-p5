package ch.zhaw.prog2.io;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


public class UnderstandingCharsets {
    public static void main(String[] args) {


        /* Teilaufgabe a
         * In der Vorlesung haben Sie gelernt, dass Java-Klassen fuer Unicode entworfen wurden.
         * Nun ist Unicode aber nicht der einzige Zeichensatz und Java unterstuetz durchaus Alternativen.
         * Welche Zeichensaetze auf einem System konkret unterstuetzt werden haengt von der Konfiguration
         * des Betriebssystems der JVM ab.
         * Schreiben Sie ein Programm, welches alle Unterstuetzten Zeichensaetze auf der Konsole (System.out) ausgibt,
         * zusammen mit dem Standardzeichensatz.
         * https://docs.oracle.com/javase/8/docs/api/java/nio/charset/Charset.html
         */

        // Print default character set
        System.out.println("Default Charset = " + Charset.defaultCharset());

        // Print all available character sets
        System.out.println("Available Charsets:");
        for (Charset charset : Charset.availableCharsets().values()) {
            System.out.println("- " + charset);
        }
        /* Ende Teilaufgabe a */


        /* Teilaufgabe b
         * Ergänzen Sie die Klasse so, dass sie einzelne Zeichen (also Zeichen für Zeichen) im Standardzeichensatz
         * von der Konsole einliest und in zwei Dateien schreibt einmal im Standardzeichensatz und einmal im
         * Zeichensatz `US-ASCII`.
         * Die Eingabe des Zeichens `q` soll das Program ordentlich beenden.
         * Die Dateien sollen `CharSetEvaluation_Default.txt` und `CharSetEvaluation_ASCII.txt` genannt und
         * werden entweder erzeugt oder, falls sie bereits existieren, geöffnet und der Inhalt überschrieben.
         * Testen Sie Ihr Program mit den folgenden Zeichen: a B c d € f ü _ q
         * Öffnen Sie die Textdateien nach Ausführung des Programs mit einem Texteditor und erklären Sie das Ergebnis.
         * Öffnen Sie die Dateien anschliessend mit einem HEX-Editor und vergleichen Sie.
         */

        char c;
        try (FileOutputStream fosDefault = new FileOutputStream("CharSetEvaluation_Default.txt");
             FileOutputStream fosAscii = new FileOutputStream("CharSetEvaluation_ASCII.txt");
             FileOutputStream fosUTF8 = new FileOutputStream("CharSetEvaluation_UTF8.txt");
             FileOutputStream fosWin = new FileOutputStream("CharSetEvaluation_WIN1252.txt");
             BufferedWriter bwdefault = new BufferedWriter(new OutputStreamWriter(fosDefault));
             BufferedWriter bwascii = new BufferedWriter(new OutputStreamWriter(fosAscii, StandardCharsets.US_ASCII));
             BufferedWriter bwutf8 = new BufferedWriter(new OutputStreamWriter(fosUTF8, StandardCharsets.UTF_8));
             BufferedWriter bwwin = new BufferedWriter(new OutputStreamWriter(fosWin, Charset.forName("windows-1252")));
             BufferedReader br = new BufferedReader(new InputStreamReader(System.in)))
        {
            System.out.println("Enter characters, 'q' to quit."); // read characters
            boolean reading = true;
            while (reading) {
                c = (char) br.read();
                /* returns character read, as an integer (32bit) in the range 0 to 65535
 		         (0x00-0xffff, 16bit), or -1 (0xffff-ffff) if the end of the stream has been reached */
                if (c == '\n') {
                    continue;
                }
                if (c == 'q') {
                    System.out.println("The End");
                    reading = false;
                } else {
                    System.out.println("== Output using Default Encoding");
                    bwdefault.write(c);
                    bwdefault.newLine();
                    bwdefault.flush();
                    System.out.println("== Output using ASCII Encoding");
                    bwascii.write(c);
                    bwascii.newLine();
                    bwascii.flush();
                    System.out.println("== Output using UTF-8 Encoding");
                    bwutf8.write(c);
                    bwutf8.newLine();
                    bwutf8.flush();
                    System.out.println("== Output using Windows-1252 Encoding");
                    bwwin.write(c);
                    bwwin.newLine();
                    bwwin.flush();
                }
                //int dummy = br.read(); //clear CRNL
            }
        } catch (IOException e) {
            System.err.println("Abbruch wegen IO-Exception" + e.getMessage());
            e.printStackTrace();
        }
    }
}

