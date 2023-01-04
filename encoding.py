import rsa
import pickle
import base64


public_file = "public_key.p"
private_file = "private_key.p"


def create_key():

    public, private = rsa.newkeys(256)
    with open(public_file, "wb") as f:
        pickle.dump(public, f)
    with open(private_file, "wb") as f:
        pickle.dump(private, f)
    print("Keys are created")


def load_key(public_file, private_file):
    with open(public_file, "rb") as f:
        public = pickle.load(f)
    with open(private_file, "rb") as f:
        private = pickle.load(f)
    return public, private


def encrypt(msg, public):
    en_msg = rsa.encrypt(msg.encode(), public)
    encoded = base64.b64encode(en_msg)
    return encoded.decode()


def decrypt(msg, private):
    de_msg = base64.b64decode(msg)
    decoded = rsa.decrypt(de_msg, private)
    return decoded.decode()


if __name__ == "__main__":
    create_key()
