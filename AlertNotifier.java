
/** Interface for sending alerts (e.g., webhook, email). */
public interface AlertNotifier {
    void notify(String message);
}
