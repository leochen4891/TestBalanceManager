import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

class Stock {
	private String ticker;
	private BigDecimal price;
	private BigDecimal amount;
	private BigDecimal balance;
	private long lastUpdate;

	public Stock(String ticker) {
		this.ticker = ticker;
		this.price = new BigDecimal(0);
		this.amount = new BigDecimal(0);
		this.balance = new BigDecimal(0);
		this.lastUpdate = -1;
	}

	public synchronized String getPNL() {
		StringBuilder sb = new StringBuilder();
		sb.append("PNL ").append(lastUpdate).append(" ").append(ticker)
				.append(" ").append(amount.toString()).append(" ")
				.append(balance.add(price.multiply(amount)).toString());
		return sb.toString();
	}

	public synchronized void updatePrice(Event event) {
		//System.out.println(event);
	}

	public synchronized void updateFill(Event event) {
		//System.out.println(event);
	}
}

class Event {
	public enum EventType {
		UNKNOWN, FILL, PRICE
	}

	private EventType type;
	private long epoch;
	private String ticker;
	private String payload;

	public Event(Event copy) {
		this.type = copy.type;
		this.epoch = copy.epoch;
		this.ticker = copy.ticker;
		this.payload = copy.payload;

	}

	public Event(String rawInput) {
		String[] strs = rawInput.split(" ", 4);
		this.type = getType(strs[0]);
		this.epoch = Long.parseLong(strs[1]);
		this.ticker = strs[2];
		this.payload = strs[3];
	}

	private EventType getType(String type) {
		if (type.equalsIgnoreCase("F"))
			return EventType.FILL;
		else if (type.equalsIgnoreCase("P"))
			return EventType.PRICE;
		return EventType.UNKNOWN;
	}

	public long getEpoch() {
		return this.epoch;
	}

	public EventType getType() {
		return this.type;
	}

	public String getTicker() {
		return this.ticker;
	}

	public String getPayload() {
		return this.payload;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.type == EventType.FILL ? "F" : "P")
				// TODO need enum to String converter
				.append(" ").append(epoch).append(" ").append(ticker)
				.append(" ").append(payload);
		return sb.toString();
	}
}

class FillEvent extends Event {
	private BigDecimal price;
	private BigDecimal amount;
	private String side;

	public FillEvent(Event base) {
		super(base);
		String[] strs = super.getPayload().split(" ", 3);
		this.price = new BigDecimal(strs[0]);
		this.amount = new BigDecimal(strs[1]);
		this.side = strs[2];
	}

	public BigDecimal getPrice() {
		return this.price;
	}

	public BigDecimal getAmount() {
		return this.amount;
	}

	public String getSide() {
		return this.side;
	}
}

class PriceEvent extends Event {
	private BigDecimal price;

	public PriceEvent(Event base) {
		super(base);
		this.price = new BigDecimal(super.getPayload());
	}

	public BigDecimal getPrice() {
		return this.price;
	}
}

interface EventReader {
	public Event getNextEvent();
}

class TwoFilesEventReader implements EventReader {

	private static int compareNextEvent(Event e1, Event e2) {
		if (e1.getType() == e2.getType()) {
			return (int) (e1.getEpoch() - e2.getEpoch());
		} else {
			if (e1.getType() == Event.EventType.PRICE)
				return -1;
			if (e2.getType() == Event.EventType.PRICE)
				return 1;
		}
		return 0;
	}

	BufferedReader br1;
	BufferedReader br2;

	Event event1;
	Event event2;

	public TwoFilesEventReader(String file1, String file2) {
		try {
			br1 = makeBufferedReader(file1);
			br2 = makeBufferedReader(file2);
			event1 = getNext(br1);
			event2 = getNext(br2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private BufferedReader makeBufferedReader(String file) throws IOException {
		BufferedReader br;
		if (file.endsWith(".gz"))
			br = new BufferedReader(new InputStreamReader(new GZIPInputStream(
					new FileInputStream(file))));
		else
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
		return br;
	}
	
	   private Event getNext(BufferedReader source) throws IOException {
	        Event res = null;
	        String line = source.readLine();
	        if (line != null) res = new Event (line);
	        return res;
	    }

	   @Override
	    public Event getNextEvent() {
	        try {
	            if (event1 == null && event2 == null) return null;
	            if (event1 == null) {
	                Event res = new Event(event2);
	                event2 = getNext(br2);
	                return res;
	            }

	            if (event2 == null) {
	                Event res = new Event(event1);
	                event1 = getNext(br1);
	                return res;
	            }

	            if (compareNextEvent(event1, event2) < 0) {
	                Event res = new Event(event1);
	                event1 = getNext(br1);
	                return res;
	            } else {
	                Event res = new Event(event2);
	                event2 = getNext(br2);
	                return res;
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	            return null;
	        }
	    }
}

public class TestBalanceManager implements Runnable {
	final static Logger logger = Logger.getLogger(TestBalanceManager.class
			.getName());
	private Map<String, Stock> stocks;
	private EventReader eventReader;

	public TestBalanceManager(String file1, String file2) {
		this.stocks = new HashMap<String, Stock>();
		this.eventReader = new TwoFilesEventReader(file1, file2);
	}

	private boolean addStock(String ticker) {
		if (null == ticker || stocks.containsKey(ticker))
			return false;

		Stock newStock = new Stock(ticker);
		this.stocks.put(ticker, newStock);
		logger.info("added new stock " + ticker);

		return true;
	}

	private boolean hasStock(String ticker) {
		return this.stocks.containsKey(ticker);
	}

	@Override
	public void run() {
		try {
			logger.info(this.getClass().getName() + " started");

			Event event = eventReader.getNextEvent();
			while (null != event) {
				String ticker = event.getTicker();

				if (!hasStock(ticker)) {
					addStock(ticker);
				}
				Stock s = stocks.get(ticker);

				switch (event.getType()) {
				case FILL:
					s.updateFill(event);
					break;

				case PRICE:
					s.updatePrice(event);
					break;

				default:
					logger.warning("got UKNOWN event type for " + ticker);
					break;
				}
				event = eventReader.getNextEvent();
			}
		} catch (Exception e) {
			// TODO
			// notify or kill
			e.printStackTrace();
		} finally {
			logger.info(this.getClass().getName() + " exited");
		}
	}

	public static void main(String[] args) {
		try {
//			TestBalanceManager balanceMgr = new TestBalanceManager("fills.txt", "prices.txt");
			TestBalanceManager balanceMgr = new TestBalanceManager("fills.gz", "prices.gz");
			Thread t = new Thread(balanceMgr);
			t.start();
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}