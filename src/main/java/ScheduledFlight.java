import java.time.LocalDateTime;
import jakarta.persistence.Entity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import io.quarkus.hibernate.reactive.panache.PanacheEntity;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
public class ScheduledFlight extends PanacheEntity {

    int claimed;
    String carrier;
    String flight;
    LocalDateTime departureDateTime;
    LocalDateTime arrivalDateTime;
    String origin;
    String destination;
}
