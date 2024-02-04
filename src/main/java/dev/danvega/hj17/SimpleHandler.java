package dev.danvega.hj17;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.*;
import com.google.firebase.messaging.*;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SimpleHandler implements RequestHandler<SQSEvent, String> {

    @Override
    public String handleRequest(SQSEvent sqsEvent, Context context) {
        try {
            FirebaseApp firebaseApp = getFirebaseApp();
            FirebaseDatabase firebaseDatabase = FirebaseDatabase.getInstance(firebaseApp);
            FirebaseMessaging firebaseMessaging = FirebaseMessaging.getInstance(firebaseApp);

            String messID = "";
            List<String> tokenDevices = new ArrayList<>();

            for (SQSEvent.SQSMessage sqsMessage : sqsEvent.getRecords()) {
                ObjectNotification objectNotification = new ObjectNotification();
                String notificationString = sqsMessage.getMessageAttributes().get("payload").getStringValue();
                String[] resultArray = notificationString.split("\\(");
                String[] payload = resultArray[1].replace(")", "").split(",");
                Class<?> notificationClass = ObjectNotification.class;

                for( String str :payload ) {
                    for (Field field : notificationClass.getDeclaredFields()) {
                        field.setAccessible(true);
                        if(str.contains(field.getName())){
                            String valueField = "";
                            int indexOfEqualSign = str.indexOf('=');

                            if (indexOfEqualSign != -1 && indexOfEqualSign < str.length() - 2) {
                                valueField =  str.substring(indexOfEqualSign + 1);

                            } else {
                                System.out.println(str.substring(indexOfEqualSign ) + " khong co gia tri");
                            }
                            field.set(objectNotification,valueField);
                        }

                    }
                }

                DatabaseReference refDb = firebaseDatabase.getReference("/User");
                Query query = refDb.orderByChild("pattern").equalTo(objectNotification.getPattern());
                query.addListenerForSingleValueEvent(new ValueEventListener() {
                    @Override
                    public void onDataChange(DataSnapshot dataSnapshot) {
                        try {
                            for (DataSnapshot userSnapshot : dataSnapshot.getChildren()) {
                                String userId = userSnapshot.getKey();
                                String userName = userSnapshot.child("userName").getValue(String.class);

                                String result2 = objectNotification.getDevices().replaceAll("[\\[\\]]", "");
                                String[] result = result2.split("; ");
                                for (String device : result) {
                                    try {
                                        List<Object> webClass = (List<Object>) userSnapshot.child("tokenDetail").child(device).getValue();
                                        for (Object obj : webClass) {
                                            if (obj != null  ) {
                                                String token = (String) ((HashMap) obj).get("token");
                                                tokenDevices.add(token);
                                            }
                                        }
                                    } catch (Exception e) {
                                        System.out.println(e);
                                    }
                                }
                                System.out.println("User ID: " + userId + ", UserName: " + userName);
                            }
                            sendMess(objectNotification, tokenDevices, firebaseMessaging);
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                    }
                    @Override
                    public void onCancelled(DatabaseError error) {
                        System.out.println("Failed to read value: " + error.toException());
                    }
                });
                Map<String, String> users = new HashMap<>();
                users.put("test", "test");
                ApiFuture<Void>  future = refDb.child("active").setValueAsync(users);
                future.get();
                ApiFuture<Void> future1 = refDb.child("active").removeValueAsync();
                future1.get();
            }
            return "success";
            } catch(Exception e){
                System.out.println(e);
                return "Error";
            }
        }


    public FirebaseApp getFirebaseApp() throws IOException {
        GoogleCredentials googleCredentials = GoogleCredentials.fromStream(
                new ClassPathResource("firebase-service-acount.json").getInputStream());
        FirebaseOptions firebaseOptions = FirebaseOptions.builder()
                .setCredentials(googleCredentials)
                .setDatabaseUrl("https://aws-combine-default-rtdb.asia-southeast1.firebasedatabase.app")
                .build();

        if(  FirebaseApp.getApps().size() == 0 ) {
            FirebaseApp app = FirebaseApp.initializeApp(firebaseOptions,"myApp");
            return app;
        }
        return FirebaseApp.getApps().get(0);
    }

    public void sendMess(ObjectNotification objectNotification, List<String> tokenDevices,FirebaseMessaging firebaseMessaging) throws ExecutionException, InterruptedException {

        Map<String, String> myMap = Map.of(
                "key1", "value1",
                "key2", "value2",
                "key3", "value3"
        );

        Notification notification = Notification.builder()
                .setTitle(objectNotification.getTitle())
                .setBody(objectNotification.getMessage())
                .setImage(objectNotification.getImage())
                .build();

        Map<String,String> headerWebConfig = new HashMap<>();
        headerWebConfig.put("Urgency","high");

        Map<String,String> dataWeb = new HashMap<>();
        dataWeb.put("webpushURL", objectNotification.getImage());

        WebpushConfig webpushConfig  = WebpushConfig.builder()
                .putAllHeaders(headerWebConfig)
                .putAllData(dataWeb)
                .build();


        com.google.firebase.messaging.MulticastMessage message = MulticastMessage
                .builder()
                .addAllTokens(tokenDevices)
                .setNotification(notification)
                .putAllData(myMap)
                .setWebpushConfig(webpushConfig)
                .build();

        ApiFuture<BatchResponse> future =  firebaseMessaging.sendMulticastAsync(message);
        BatchResponse result = future.get();
        System.out.println("thanh cong: " + result.getSuccessCount() + ". that bai : " + result.getFailureCount() );
    }

}
