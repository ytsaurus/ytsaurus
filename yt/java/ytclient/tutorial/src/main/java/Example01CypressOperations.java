import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.request.ColumnFilter;
import ru.yandex.yt.ytclient.proxy.request.GetNode;

public class Example01CypressOperations {
    public static void main(String[] args) {
        // *** Создание клиента ***

        // Удобнее всего создать клиент через builder.
        // Единственный обязательный параметр, который надо указать, это кластер.
        //
        // YT токен подхватится из ~/.yt/token, а имя пользователя из операционной системы.
        YtClient client = YtClient.builder()
                .setCluster("freud")
                .build();

        // Для корректного завершения работы у клиента необходимо позвать close()
        // Надёжнее и удобнее всего это сделать с помощью конструкции try-with-resources.
        try (client) {
            // *** Простые запросы ***

            // Большинство методов клиента возвращают CompletableFuture
            // Сам запрос будет послан в фоне.
            CompletableFuture<YTreeNode> listResult = client.listNode("/");

            // Чтобы дождаться результата выполенения запроса можно использовать метод join.
            // Ну и вообще стоит почитать доку про CompletableFuture (если ещё не):
            // https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/CompletableFuture.html
            System.out.println("listResult: " + listResult.join());

            // *** Использование необязательных параметров в запросе ***

            // Для всех запросов есть полная версия метода, где можно передать объект запроса, указав там продвинутые
            // опции.
            CompletableFuture<YTreeNode> getResult = client.getNode(
                    new GetNode("//home/dev/tutorial")
                    .setAttributes(ColumnFilter.of("account", "row_count"))
                    .setTimeout(Duration.ofSeconds(10))
            );
            System.out.println("getResult: " + getResult.join());

            // *** Ошибки ***

            // Вызовы методов не будут бросать исключений (может быть за исключением случаев, когда имеет место какая-то
            // программная ошибка или мисконфигурация).
            CompletableFuture<YTreeNode> badListResult = client.listNode("//some/directory/that/does/not/exist");

            try {
                // Если в результате выполнения запроса YT вернёт ошибку или случится сетевая ошибка, то соответствующее
                // исключение будет сохранено в CompletableFuture.
                badListResult.join();
            } catch (CompletionException ex) {
                System.out.println("ERROR: " + ex);
            }
        }
    }
}
