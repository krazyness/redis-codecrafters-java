import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      clientSocket = serverSocket.accept();

      InputStream inputStream = clientSocket.getInputStream();
      byte[] buffer = new byte[1024];
      int bytesRead = inputStream.read(buffer);

      while (true) {
        if (bytesRead == 0) {
          break;
        }
        OutputStream outputStream = clientSocket.getOutputStream();
        outputStream.write("+PONG\r\n".getBytes());
      }
      
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
