import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hello world!
 */
public class App {

    private static Destination _replyTo;
    private static String REV_QUEUE = "STRESS.TEST";
    private static String RPY_QUEUE = "STRESS.TEST_R";
    private static String AMQ_IP = "tcp://";
    private static String startTime = "15:00"; //"23:00";
    private static String endTime = "16:35"; //"06:00";
    private static boolean isInit = true;
    private static AtomicInteger proCount = new AtomicInteger();
    private static AtomicInteger conCount = new AtomicInteger();
    //                SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");

    public static void main(String[] args) throws Exception {

        Properties properties = System.getProperties();

        SimpleDateFormat sdf2 = new SimpleDateFormat("YY-MM-dd HH:mm");

        Calendar currentCal = Calendar.getInstance();
        Calendar endDateCal = Calendar.getInstance();
        endDateCal.add(Calendar.DAY_OF_MONTH, 7);
//        endDateCal.add(Calendar.MINUTE, 5);

        String endDate = sdf2.format(endDateCal.getTime());
        String currentDate = sdf2.format(currentCal.getTime());
        // 7 days

        int producerCount = Integer.parseInt(properties.getProperty("producerCount"));
        int consumerCount = Integer.parseInt(properties.getProperty("consumerCount"));
        AMQ_IP += properties.getProperty("amqIp");
        startTime = properties.getProperty("startTime");
        endTime = properties.getProperty("endTime");

        System.out.println("producer count:" + producerCount);
        System.out.println("consumer count:" + consumerCount);

        System.out.println("==== Start ====");
        System.out.println("current :" + currentDate);
        System.out.println("end :" + endDate);

        while (currentCal.before(endDateCal)) {
            if (isInit) {
                for (int i = 0; i < producerCount; i++) {
                    thread(new HelloWorldProducer(), false);
                    proCount.incrementAndGet();
                }
                for (int i = 0; i < consumerCount; i++) {
                    thread(new HelloWorldConsumer(), false);
                    conCount.incrementAndGet();
                }
                isInit = false;
            } else if (!isInit && proCount.get() == 0 && conCount.get() == 0 && App.checkWorkingTime(currentCal)) {
                isInit = true;
            }
            currentCal = Calendar.getInstance();
            currentDate = sdf2.format(currentCal.getTime());
        }
        System.out.println("==== Stop ====");
        System.out.println("current :" + currentDate);
        System.out.println("end :" + endDate);
    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
    public static boolean checkWorkingTime(Calendar currentCal) {
        String[] parts = startTime.split(":");
        Calendar startCal = Calendar.getInstance();
        startCal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(parts[0]));
        startCal.set(Calendar.MINUTE, Integer.parseInt(parts[1]));

        parts = endTime.split(":");
        Calendar endCal = Calendar.getInstance();
        endCal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(parts[0]));
        endCal.set(Calendar.MINUTE, Integer.parseInt(parts[1]));

        boolean isWorkingTime = currentCal.after(startCal) && currentCal.before(endCal);
        return isWorkingTime;
    }
    public static class HelloWorldProducer implements Runnable {
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(AMQ_IP);

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue(REV_QUEUE);

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                producer.setTimeToLive(1000);

                Calendar currentCal = Calendar.getInstance();

                // 23:00 ~ 06:00
                while (App.checkWorkingTime(currentCal)) {
//                    String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + UUID.randomUUID();

                    String text = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                            "<message>\n" +
                            "     <message_id>EQP_STATUS_CHANGE_REPORT</message_id>\n" +
                            "     \t<type_id>I</type_id>\n" +
                            "\t<system_byte> N01-EQP_STATUS_CHANGE_REPORT-20170603142001789</system_byte>\n" +
                            "     \t<occur_time>20170903192447</occur_time>\n" +
                            "     \t<line_id>TEST</line_id>\n" +
                            "     \t<line_status>RUN</line_status>\n" +
                            "\t<eqp_list>\n" +
                            "\t\t<eqp>\n" +
                            "     \t\t\t<eqp_id>TEST-N01</eqp_id>\n" +
                            "     \t\t\t<eqp_status>1000</eqp_status>\n" +
                            "     \t\t\t\t\t<occur_time>20170903192447</occur_time>\n" +
                            "</eqp>\n" +
                            "</eqp_list>\n" +
                            "  <sub_eqp_list>\n" +
                            "  \t<sub_eqp>\n" +
                            "\t\t\t<sub_eqp_id>CH01</sub_eqp_id>\n" +
                            "\t\t\t<sub_eqp_status>1000</sub_eqp_status>\n" +
                            "    </sub_eqp>\n" +
                            "  </sub_eqp_list>\n" +
                            "</message>\n";

                    text = text.replace(" ", "").replace("\t", "").replace("\n", "").replace("\r", "");

                    TextMessage message = session.createTextMessage(text);
//                    Destination replyTo = session.createQueue(RPY_QUEUE);
//                    message.setJMSReplyTo(replyTo);

                    // Tell the producer to send the message
                    System.out.println(currentCal.getTime() + " Sent message... " + Thread.currentThread().getName());
                    producer.send(message);

                    Thread.sleep(5000);
                    // update
//                    System.out.println("current date:" + sdf.format(currentCal.getTime()));
                    currentCal = Calendar.getInstance();
                }
                session.close();
                connection.close();
                proCount.decrementAndGet();
                System.out.println(currentCal.getTime() + "*** producer stopped.");
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }

    public static class HelloWorldConsumer implements Runnable, ExceptionListener {
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(AMQ_IP);

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                connection.setExceptionListener(this);

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue(REV_QUEUE);

                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer(destination);

                Calendar currentCal = Calendar.getInstance();

                // Wait for a message
                while (App.checkWorkingTime(currentCal)) {
                    Message message = consumer.receive(1000);

                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        String text = textMessage.getText();
                        System.out.println(currentCal.getTime() + " Received: " + text);

                        Destination replyTo = message.getJMSReplyTo();
                        if (replyTo != null) {
                            MessageProducer producer = session.createProducer(replyTo);
                            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                            producer.setTimeToLive(1);
                            String txt = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                                    "<message>\n" +
                                    "\t<message_id>TIME_SYNC_REQUEST_R</message_id>\n" +
                                    "\t<type_id>O</type_id>\n" +
                                    "\t<system_byte>TIME_SYNC_REQUEST-12301000000000</system_byte>\n" +
                                    "<rtn_code>0000000</rtn_code>\n" +
                                    "\t<rtn_msg>msg</rtn_msg>\n" +
                                    "\t<mes_time>20170903192447</mes_time>\n" +
                                    "</message>\n";
                            TextMessage msg = session.createTextMessage(txt);
                            producer.send(msg);
                        }
                    } else {
                        System.out.println(currentCal.getTime() + " Received: " + message);
                    }

                    // update
//                    System.out.println("current date:" + sdf.format(currentCal.getTime()));
                    currentCal = Calendar.getInstance();
                }
                session.close();
                connection.close();
                conCount.decrementAndGet();
                System.out.println(currentCal.getTime() + "*** consumer stopped.");
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}