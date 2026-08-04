use arrow_flight::error::FlightError;

pub(crate) fn to_flight_error(e: impl std::fmt::Display) -> FlightError {
    FlightError::ExternalError(Box::new(std::io::Error::other(
        e.to_string(),
    )))
}
