use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

type AccountId = u32;
type HandleId = i32;
type TxCount = u32;

const INVALID_HANDLE: HandleId = -1;
const THREAD_COUNT: usize = 4;

struct Tx {
    account: AccountId,
    amount: u32,
    tx_type: TxType,
}

struct TxHandler {
    sender: Sender<Message>,
    thread: Option<thread::JoinHandle<()>>,
}
struct ServerData {
    pending_tx: HashMap<AccountId, TxCount>, // account -> pending tx count
    tx_count: HashMap<HandleId, TxCount>,    // handler id -> pending tx count
    handler: HashMap<AccountId, HandleId>,   // account -> handler id
    balances: HashMap<AccountId, u32>,       // account -> balance
}

enum Message {
    NewTx(Tx),
    Terminate,
}

#[derive(Debug, PartialEq, Eq)]
enum TxType {
    DEPOSIT,
    WITHDRAW,
}

struct Aptone {
    server_data: Arc<Mutex<ServerData>>,
    handles: Vec<TxHandler>,
}

impl ServerData {
    fn increase_pending_tx(&mut self, account: AccountId, amount: TxCount) -> TxCount {
        let pending = self.pending_tx.entry(account).or_insert(0);
        *pending += amount;
        *pending
    }
    fn decrease_pending_tx(&mut self, account: AccountId, amount: TxCount) -> TxCount {
        match self.pending_tx.get_mut(&account) {
            None => 0,
            Some(pending) => {
                if *pending > amount {
                    *pending -= amount;
                } else {
                    *pending = 0;
                }
                *pending
            }
        }
    }
    fn get_pending_tx(&self, account: AccountId) -> TxCount {
        match self.pending_tx.get(&account) {
            None => 0,
            Some(pending) => *pending,
        }
    }
    fn increase_tx_count(&mut self, handle_id: HandleId, amount: TxCount) {
        *self.tx_count.entry(handle_id).or_insert(0) += amount;
    }
    fn decrease_tx_count(&mut self, handle_id: HandleId, amount: TxCount) {
        match self.tx_count.get_mut(&handle_id) {
            None => {}
            Some(count) => {
                if *count > amount {
                    *count -= amount;
                } else {
                    *count = 0;
                }
            }
        }
    }
    fn increase_balance(&mut self, account: AccountId, amount: u32) {
        *self.balances.entry(account).or_insert(0) += amount;
    }
    fn decrease_balance(&mut self, account: AccountId, amount: u32) {
        match self.balances.get_mut(&account) {
            None => {
                panic!("balance entry does not exist for account: {}", account);
            }
            Some(balance) => {
                if *balance < amount {
                    panic!("Insufficient balance!");
                } else {
                    *balance -= amount;
                }
            }
        }
    }
    fn get_balance(&self, account: AccountId) -> u32 {
        if let Some(x) = self.balances.get(&account) {
            *x
        } else {
            // panic!("account {} does not exist!", account);
            0
        }
    }
    fn set_handle(&mut self, account: AccountId, handle_id: HandleId) {
        assert!(handle_id == INVALID_HANDLE || handle_id >= 0);
        *self.handler.entry(account).or_insert(INVALID_HANDLE) = handle_id;
    }
    fn get_handle(&mut self, account: AccountId) -> HandleId {
        let current_handle = self.handler.entry(account).or_insert(INVALID_HANDLE);
        if *current_handle != INVALID_HANDLE {
            *current_handle
        } else {
            let mut hid: HandleId = INVALID_HANDLE;
            let mut min_count: TxCount = TxCount::MAX;

            for id in 0..THREAD_COUNT {
                let count = *self.tx_count.entry(id as HandleId).or_insert(0);

                if count < min_count {
                    min_count = count;
                    hid = id as HandleId;
                }
            }

            hid
        }
    }
}

impl TxHandler {
    fn new(
        id: HandleId,
        sender: Sender<Message>,
        receiver: Receiver<Message>,
        server_data: Arc<Mutex<ServerData>>,
    ) -> TxHandler {
        let thread = thread::spawn(move || loop {
            let message = receiver.recv().unwrap();

            match message {
                Message::NewTx(Tx {
                    account,
                    amount,
                    tx_type,
                }) => {
                    {
                        let mut data = server_data.lock().unwrap();
                        match tx_type {
                            TxType::DEPOSIT => {
                                data.increase_balance(account, amount);
                            }
                            TxType::WITHDRAW => {
                                data.decrease_balance(account, amount);
                            }
                        }
                        if data.decrease_pending_tx(account, 1) == 0 {
                            data.set_handle(account, INVALID_HANDLE);
                        }
                        data.decrease_tx_count(id, 1);
                    }
                    thread::sleep(Duration::from_millis(500)); // forcing delay for experimental purpose
                }
                Message::Terminate => {
                    println!("Terminating thread {}", id);
                    break;
                }
            }
        });
        TxHandler {
            sender,
            thread: Some(thread),
        }
    }
}

impl Aptone {
    fn new() -> Aptone {
        let mut handlers = Vec::with_capacity(THREAD_COUNT);

        let pending_tx = HashMap::new();
        let tx_count = HashMap::new();
        let handler = HashMap::new();
        let balances = HashMap::new();
        let server_data = ServerData {
            pending_tx,
            tx_count,
            handler,
            balances,
        };
        let server_data = Arc::new(Mutex::new(server_data));

        for id in 0..THREAD_COUNT {
            let (sender, receiver) = channel::<Message>();
            let shared = Arc::clone(&server_data);
            handlers.push(TxHandler::new(id as HandleId, sender, receiver, shared));
        }
        Aptone {
            server_data,
            handles: handlers,
        }
    }
    fn handle_tx(&self, account: u32, amount: u32, tx_type: TxType) {
        let mut data = self.server_data.lock().unwrap();

        let id = data.get_handle(account);
        println!(
            "account: {} \t balance = {}\t pending = {} \t amount: {} \t type: {:?} --> {}",
            account,
            data.get_balance(account),
            data.get_pending_tx(account),
            amount,
            tx_type,
            id
        );
        data.set_handle(account, id);
        data.increase_pending_tx(account, 1);
        data.increase_tx_count(id, 1);

        assert!(id != INVALID_HANDLE);

        self.handles[id as usize]
            .sender
            .send(Message::NewTx(Tx {
                account,
                amount,
                tx_type,
            }))
            .unwrap();
    }
    fn withdraw(&self, account: AccountId, amount: u32) {
        self.handle_tx(account, amount, TxType::WITHDRAW);
    }
    fn deposit(&self, account: AccountId, amount: u32) {
        self.handle_tx(account, amount, TxType::DEPOSIT);
    }
    fn get_balance(&self, account: AccountId) -> u32 {
        self.server_data.lock().unwrap().get_balance(account)
    }
}

impl Drop for Aptone {
    fn drop(&mut self) {
        println!("--- Killing all threads...");
        for handler in &mut self.handles {
            println!("          Sending termination message...");
            handler.sender.send(Message::Terminate).unwrap();
        }

        for handler in &mut self.handles {
            if let Some(thread) = handler.thread.take() {
                thread.join().unwrap();
            } else {
                println!("oops");
            }
        }
    }
}

fn main() {
    let aptone = Arc::new(Mutex::new(Aptone::new()));
    let aptone_one = Arc::clone(&aptone);
    let simulator = thread::spawn(move || {
        for _ in 0..4 {
            let aptone = aptone_one.lock().unwrap();
            aptone.deposit(0, 500);
            aptone.deposit(1, 400);
            aptone.withdraw(1, 300);
            aptone.withdraw(0, 100);
            thread::sleep(Duration::from_millis(700));
        }
    });
    simulator.join().unwrap();

    println!("wait until all transactions are finished");
    thread::sleep(Duration::from_secs(10));
    {
        let aptone = Arc::clone(&aptone);
        let aptone = aptone.lock().unwrap();
        for account in 0..2 {
            println!(
                "account: {} \t balance: {}",
                account,
                aptone.get_balance(account)
            );
        }
    }
    println!("Terminating program...");
}
