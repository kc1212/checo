class TrustChain:
    """
    Node maintains one TrustChain object and interacts with it either in in the reactor process or some other process.
    If it's the latter, there needs to be some communication mechanism.

    type System = Map<Node, Chain>;

    // height (sequence number) should match the index
    type Chain = List<Block>;

    struct Node {
        pk: [u8; 32],
        addr: SocketAddr,
        // ...
    }

    struct Signature {
        pk: [u8; 32],
        sig: [u8, 32],
    }

    enum Block {
        TxBlock,
        CpBlock,
    }

    struct TxBlock {
        prev: Digest,
        h_s: u64,
        h_r: u64,
        s_s: Signature,
        s_r: Signature,
        m: String,

        // items below are not a part of the block digest
        validity: Valid | Invalid | Unknown
    }

    struct Cons {
        round: u64,
        blocks: List<CpBlock>,
        ss: List<Signature>,
    }

    struct CpBlock {
        prev: Digest,
        round: u64, // of the Cons
        con: Digest, // of the Cons
        p: bool, // promoter registration
        s: Signature,
    }

    """

    def __init__(self):
        self.myself = None
        self.system = None

    def new_tx(self, tx):
        """
        Verify tx, follow the rule and mutates the state to add it
        :return: None
        """
        pass

    def new_cp(self, cp):
        """
        Verify the cp, follow the rule a nd mutate the state to add it
        :return: None
        """
        pass

    def pieces(self, tx):
        """
        tx must exist, return the pieces of tx
        :param tx:
        :return: List<Block>
        """
        pass

    def verify(self, tx, resp):
        """

        :param tx:
        :param resp:
        :return:
        """
        pass

    def _enclosure(self, tx):
        """

        :param tx:
        :return: (CpBlock, CpBlock)
        """
        pass