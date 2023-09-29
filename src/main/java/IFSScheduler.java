import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.logging.Log;
import io.quarkus.panache.common.Parameters;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.vertx.VertxContextSupport;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@ApplicationScoped
public class IFSScheduler {

        @Inject
        io.vertx.mutiny.pgclient.PgPool client;

        boolean dbInitialized = false;

        @Scheduled(every = "5s")
        void runTask() {

                if (!dbInitialized) {
                        try {
                                VertxContextSupport.subscribeAndAwait(() -> {
                                        dbInitialized = true;
                                        return initializeDB();
                                });
                        } catch (Throwable e) {
                                e.printStackTrace();
                        }

                        Log.info("\n\n\n\n");
                }

                try {
                        VertxContextSupport.subscribeAndAwait(() -> {
                                return processNextUnclaimedFllight(LocalDateTime.now().plusHours(4));
                        });
                        VertxContextSupport.subscribeAndAwait(() -> {
                                return processNextUnclaimedFllight(LocalDateTime.now().plusMinutes(40));
                        });
                } catch (Throwable e) {
                        e.printStackTrace();
                }

        }

        @WithSession
        Uni<Boolean> processNextUnclaimedFllight(LocalDateTime departureTime) {

                return findNextUnclaimedFlight(departureTime)
                                .onItem()
                                .<ScheduledFlight>transformToUni(f -> {
                                        return claimFlight(f)
                                                        .onItem()
                                                        .<ScheduledFlight>transform((b) -> f);
                                })
                                .onItem()
                                .invoke(f -> processFlight(f))
                                .onItem()
                                .transform(f -> true)
                                .onFailure().recoverWithItem(false);

        }

        @WithSession
        Uni<ScheduledFlight> findNextUnclaimedFlight(LocalDateTime departureTime) {

                return ScheduledFlight
                                .<ScheduledFlight>find(
                                                "from ScheduledFlight f where f.claimed = :claimed and f.departureDateTime > :before and f.departureDateTime < :after",
                                                Parameters.with("claimed", 0)
                                                                .and("before", departureTime.minusMinutes(1))
                                                                .and("after", departureTime))
                                .firstResult();
        }

        Uni<RowSet<Row>> claimFlight(ScheduledFlight flight) {

                return client.preparedQuery("UPDATE scheduledflight SET claimed = 1 WHERE id = $1")
                                .execute(Tuple.of(flight.id));
        }

        String processFlight(ScheduledFlight f) {
                Log.info(String.format("\nProcess Flight: %s\tDepart: %s\tProcessed at: %s\n", f.flight,
                                f.departureDateTime.toString(), LocalDateTime.now().toString()));
                return f.flight;
        }

        @WithSession
        @Transactional(TxType.REQUIRES_NEW)
        Uni<Boolean> initializeDB() {

                Log.info("Init DB");

                var c = new ArrayList<ScheduledFlight>();

                Collections.addAll(c,
                                ScheduledFlight.builder().carrier("DL").flight("1").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusHours(4).minusMinutes(2))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("2").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusMinutes(40).minusSeconds(30))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("3").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusHours(4))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("4").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusMinutes(40).plusMinutes(1))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("5").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusHours(4).plusMinutes(1))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("6").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusMinutes(40).plusMinutes(1))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("7").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusHours(4).plusMinutes(4))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("8").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusMinutes(40).plusMinutes(5))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("9").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusHours(4).plusMinutes(6))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("10").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusMinutes(40).plusMinutes(7))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("11").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusHours(4).plusMinutes(8))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("12").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusMinutes(40).plusMinutes(9))
                                                .build(),
                                ScheduledFlight.builder().carrier("DL").flight("13").claimed(0)
                                                .departureDateTime(LocalDateTime.now().plusHours(4).plusMinutes(10))
                                                .build());

                // Odd code because when adding a collection of records with persist, there is
                // no persistAndFlush for a collection. Without flushing, the records never show
                // up in the database because it is part of the same transaction (or something
                // else; I use REQUIRED_NEW but it still doesn't flush or commit.) So, I add one
                // more record so I can persistAndFlush. When that happens the records show up
                // in the database.
                return ScheduledFlight.persist(c)
                                .onItem()
                                .<ScheduledFlight>transformToUni(
                                                (f) -> ScheduledFlight.builder().carrier("DL").flight("14")
                                                                .departureDateTime(LocalDateTime.now().plusMinutes(40)
                                                                                .plusMinutes(10))
                                                                .build().persistAndFlush())
                                .onItem()
                                .transform((v) -> true)
                                .onFailure()
                                .recoverWithItem(v -> false);

        }

}
