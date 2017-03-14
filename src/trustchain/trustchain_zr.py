
class TrustChainZR:
    def __init__(self):
        self.size = None
        self.id = None
        """
        TxMsg {
            ?
        }
        TxBlock {
            prev: Digest,
            tx_msgs: List<TxMsg>,
        }
        Cons {
            ?
        }
        CpBlock {
            prev: Digest,
            cons: Cons,
        }
        """
        self.chains = {}  # key: id, value: list of blocks

    def add_tx(self):
        pass

    def add_cp(self):
        pass

    def add_txs(self):
        pass


