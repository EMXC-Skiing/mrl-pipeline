import time

from gspread import Spreadsheet, Worksheet
from gspread.exceptions import APIError


class RetryingMethod:
    """A wrapper for methods that adds retry logic with exponential backoff for handling
    specific API errors.

    This class enables resilience by retrying a given function up to a defined number of
    attempts when an `APIError` occurs, specifically for HTTP status codes 503 (service
    unavailable), 500 (internal server error), and 429 (rate limiting).

    Attributes:
        function (Callable): The function to be wrapped with retry logic. retries (int):
        The maximum number of retry attempts allowed. delay (float): The initial delay
        (in seconds) between retry attempts. backoff_factor (float): The exponential
        backoff factor applied to the delay for each retry attempt.

    Methods:
        __call__(*args, **kwargs): Invokes the wrapped function with retry logic,
        handling specified API errors
                                   with exponential backoff and conditional delays.

    """

    def __init__(self, function, retries, delay, backoff_factor):
        self.function = function
        self.retries = retries
        self.delay = delay
        self.backoff_factor = backoff_factor

    def __call__(self, *args, **kwargs):
        attempt = 0
        while attempt < self.retries:
            try:
                return self.function(*args, **kwargs)
            except APIError as e:  # noqa: PERF203
                # Access the HTTP status code
                error_code = e.response.status_code

                if (error_code in (503, 500)):
                    attempt += 1
                    if attempt < self.retries:
                        print(
                            f"APIError {error_code} encountered. Retrying "
                            f"{attempt}/{self.retries} with backoff...",
                        )
                        time.sleep(self.delay * (self.backoff_factor ** (attempt - 1)))
                    else:
                        print(
                            f"Max retries exceeded after {self.retries} "
                            "attempts for 503 error.",
                        )
                        raise
                elif error_code == 429:
                    attempt += 1
                    if attempt < self.retries:
                        print(
                            f"APIError 429 encountered. Retrying {attempt}"
                            f"/{self.retries} after 60 seconds...",
                        )
                        time.sleep(60)
                    else:
                        print(
                            f"Max retries exceeded after {self.retries} "
                            "attempts for 429 error.",
                        )
                        raise
                else:
                    raise

        return None


# create a wrapper for an API client that retries API requests on specific errors
class ResilientGspreadClient:
    """A resilient wrapper for an API client instance, applying retry logic to API calls
    and propagating itself to returned instances of specified classes.

    This class wraps an instance of an API client (e.g., `gspread.Client`) and
    intercepts API method calls to handle specified errors with retry logic, including
    exponential backoff. It automatically wraps instances of certain classes (e.g.,
    `Spreadsheet`, `Worksheet`) returned by API methods, ensuring that these objects
    inherit the same retry resilience.

    Attributes:
        base_client_instance (object): The API client instance to be wrapped. retries
        (int): The maximum number of retry attempts for each API method. delay (float):
        The initial delay (in seconds) between retry attempts. backoff_factor (float):
        The exponential backoff factor applied to the delay for each retry.
        propagating_classes (list): A list of class types from the API client that
        should be wrapped with retry logic if they are returned as results of API calls.

    Methods:
        __getattr__(name): Retrieves an attribute from the wrapped instance, applying
        retry logic if the attribute is callable (i.e., it's an API method). If the
        result is an instance of a propagating class or an iterable
        `ResilientGspreadClient` to propagate resilient behavior.
        containing instances of these classes, it is wrapped in
        _is_iterable(obj): Checks if an object is iterable, excluding strings and bytes.


    Usage:
        client = Client(...)  # Assume an instance of gspread.Client or similar
        resilient_client = ResilientGspreadClient(client, retries=10, delay=5,
        backoff_factor=2) spreadsheet = resilient_client.open("Spreadsheet Name")  #
        Example API call with retry logic

    Example:
        After calling an API method like `open`, the returned `spreadsheet` object (if
        it's an instance of `Spreadsheet`) will automatically be wrapped in
        `ResilientGspreadClient`. Any further method calls on `spreadsheet`, such as
        accessing worksheets, will also use retry logic if they return propagating class
        instances or iterables of them, maintaining resilience across nested API calls.

    """

    def __init__(
        self,
        base_client_instance,
        retries=10,
        delay=5,
        backoff_factor=2,
        propagating_classes=None,
    ):
        self._base_client_instance = base_client_instance
        self._retries = retries
        self._delay = delay
        self._backoff_factor = backoff_factor
        self._propagating_classes = propagating_classes or [Spreadsheet, Worksheet]

    def __getattr__(self, name):
        # Retrieve the attribute from the wrapped instance
        attr = getattr(self._base_client_instance, name)

        # If the attribute is callable (i.e., it's a method), wrap it
        if callable(attr):
            # we will return wrapped_method, a wrapped version of the base method
            def wrapped_method(*args, **kwargs):
                # Call the original method with reslient retries
                resilient_attr = RetryingMethod(
                    attr, self._retries, self._delay, self._backoff_factor,
                )
                result = resilient_attr(*args, **kwargs)

                # If the result is a class instance that should be wrapped, do that and
                # return it
                if any(isinstance(result, cls) for cls in self._propagating_classes):
                    # return the wrapped object
                    r = ResilientGspreadClient(
                        result,
                        self._retries,
                        self._delay,
                        self._backoff_factor,
                        self._propagating_classes,
                    )
                    return r

                # If the result is iterable then try to propagate ResilientGspreadClient
                # to its elements
                if self._is_iterable(result):

                    def wrapped_iterator():
                        for item in result:
                            if any(
                                isinstance(item, cls)
                                for cls in self._propagating_classes
                            ):
                                yield ResilientGspreadClient(
                                    item,
                                    self._retries,
                                    self._delay,
                                    self._backoff_factor,
                                    self._propagating_classes,
                                )
                            else:
                                yield item

                    # provide a wrapped iterator, propagating ResilientGspreadClient to
                    # elements as appropriate
                    return wrapped_iterator()

                # if the result of the called method is not a class instance or an
                # iterable, return the result as-is
                return result

            # return the base method with wrapper
            return wrapped_method

        # Return attribute of base class directly if it's not callable
        return attr

    def _is_iterable(self, obj):
        # Exclude strings and bytes, then check for the __iter__ method
        return not isinstance(obj, (str, bytes)) and hasattr(obj, "__iter__")
