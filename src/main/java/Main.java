import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        Thread clientThread = new Thread(() -> clientHandler(clientSocket));
        clientThread.start();
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (serverSocket != null) {
          serverSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  private static void clientHandler(Socket clientSocket) {
    try {
      OutputStream outputStream = clientSocket.getOutputStream();

      while (true) {
        byte[] buffer = new byte[1024];
        clientSocket.getInputStream().read(buffer);
        String input = new String(buffer);
        outputStream.write(handleCommand(input).getBytes());
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        clientSocket.close();
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  private static String handleCommand(String input) {
    String[] lines = input.trim().split("\r\n");

    String command = "";
    String message = "";
    
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].startsWith("$") && i + 1 < lines.length) {
        if (command.isEmpty()) {
          command = lines[i + 1];
        } else if (command.equalsIgnoreCase("echo")) {
          message = lines[i + 1];
          break;
        }
      }
    }

    if (command.equalsIgnoreCase("echo")) {
      return "$" + message.length() + "\r\n" + message + "\r\n";
    } else if (command.equals("ping")) {
      return "+PONG\r\n";
    }
    return "+PONG\r\n";
  }
}
