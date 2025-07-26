import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class Main {
  private static Map<String, String> data = new HashMap<>();

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
    System.out.println(input);
    String[] lines = input.trim().split("\r\n");

    String command, key, value;
    command = key = value = "";

    int loopRun = 0;
    
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].startsWith("$") && i + 1 < lines.length) {
        if (command.isEmpty()) {
          command = lines[i + 1];
        } else {
          loopRun++;
          if (command.equalsIgnoreCase("echo") && loopRun == 1) {
            key = lines[i + 1];
            break;
          } else if (command.equalsIgnoreCase("set")) {
            if (loopRun == 1) {
              key = lines[i + 1];
              System.out.println("key: " + key);
            } else if (loopRun == 2) {
              value = lines[i + 1];
              System.out.println("set: " + value);
              break;
            }
          } else if (command.equalsIgnoreCase("get") && loopRun == 1) {
            key = lines[i+1];
            break;
          }
        }
      }
    }

    switch (command) {
      case "ping":
        return "+PONG\r\n";
      case "echo":
        return "$" + key.length() + "\r\n" + key + "\r\n";
      case "set":
        data.put(key, value);
        return "+OK\r\n";
      case "get":
        String storedValue = data.get(key);
        if (storedValue == null) {
          return "$-1\r\n";
        }
        return "$" + storedValue.length() + "\r\n" + storedValue;
      default:
        return "+PONG\r\n";
    }
  }
}
