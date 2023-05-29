package com.jm.dispatch.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public final class BufferedRandomAccessFile extends RandomAccessFile {
    static final int LOG_BUFF_SZ = 16;
    public static final int BUFF_SZ = 65536;
    static final long BUFF_MASK = -65536L;
    private boolean dirty_;
    private boolean closed_;
    private long curr_;
    private long lo_;
    private long hi_;
    private byte[] buff_;
    private long maxHi_;
    private boolean hitEOF_;
    private long diskPos_;
    
    public BufferedRandomAccessFile(File file, String mode) throws IOException {
        super(file, mode);
        this.init(0);
    }
    
    public BufferedRandomAccessFile(File file, String mode, int size) throws IOException {
        super(file, mode);
        this.init(size);
    }
    
    public BufferedRandomAccessFile(String name, String mode) throws IOException {
        super(name, mode);
        this.init(0);
    }
    
    public BufferedRandomAccessFile(String name, String mode, int size) throws FileNotFoundException {
        super(name, mode);
        this.init(size);
    }
    
    private void init(int size) {
        this.dirty_ = this.closed_ = false;
        this.lo_ = this.curr_ = this.hi_ = 0L;
        this.buff_ = size > 65536 ? new byte[size] : new byte[65536];
        this.maxHi_ = 65536L;
        this.hitEOF_ = false;
        this.diskPos_ = 0L;
    }
    
    @Override
    public void close() throws IOException {
        this.flush();
        this.closed_ = true;
        super.close();
    }
    
    public boolean isClosed() {
        return this.closed_;
    }
    
    public void flush() throws IOException {
        this.flushBuffer();
    }
    
    private void flushBuffer() throws IOException {
        if (this.dirty_) {
            if (this.diskPos_ != this.lo_) {
                super.seek(this.lo_);
            }
            
            int len = (int)(this.curr_ - this.lo_);
            super.write(this.buff_, 0, len);
            this.diskPos_ = this.curr_;
            this.dirty_ = false;
        }
        
    }
    
    private int fillBuffer() throws IOException {
        int cnt = 0;
        
        int n;
        for(int rem = this.buff_.length; rem > 0; rem -= n) {
            n = super.read(this.buff_, cnt, rem);
            if (n < 0) {
                break;
            }
            
            cnt += n;
        }
        
        if (cnt < 0 && (this.hitEOF_ = cnt < this.buff_.length)) {
            Arrays.fill(this.buff_, cnt, this.buff_.length, (byte)-1);
        }
        
        this.diskPos_ += (long)cnt;
        return cnt;
    }
    
    @Override
    public void seek(long pos) throws IOException {
        if (pos < this.hi_ && pos >= this.lo_) {
            if (pos < this.curr_) {
                this.flushBuffer();
            }
        } else {
            this.flushBuffer();
            this.lo_ = pos & -65536L;
            this.maxHi_ = this.lo_ + (long)this.buff_.length;
            if (this.diskPos_ != this.lo_) {
                super.seek(this.lo_);
                this.diskPos_ = this.lo_;
            }
            
            int n = this.fillBuffer();
            this.hi_ = this.lo_ + (long)n;
        }
        
        this.curr_ = pos;
    }
    
    public long getFilePointer() {
        return this.curr_;
    }
    
    public long length() throws IOException {
        return Math.max(this.curr_, super.length());
    }
    
    public int read() throws IOException {
        if (this.curr_ >= this.hi_) {
            if (this.hitEOF_) {
                return -1;
            }
            
            this.seek(this.curr_);
            if (this.curr_ == this.hi_) {
                return -1;
            }
        }
        
        byte res = this.buff_[(int)(this.curr_ - this.lo_)];
        ++this.curr_;
        return res & 255;
    }
    
    public int read(byte[] b) throws IOException {
        return this.read(b, 0, b.length);
    }
    
    public int read(byte[] b, int off, int len) throws IOException {
        if (this.curr_ >= this.hi_) {
            if (this.hitEOF_) {
                return -1;
            }
            
            this.seek(this.curr_);
            if (this.curr_ == this.hi_) {
                return -1;
            }
        }
        
        len = Math.min(len, (int)(this.hi_ - this.curr_));
        int buffOff = (int)(this.curr_ - this.lo_);
        System.arraycopy(this.buff_, buffOff, b, off, len);
        this.curr_ += (long)len;
        return len;
    }
    
    public void write(int b) throws IOException {
        if (this.curr_ >= this.hi_) {
            if (this.hitEOF_ && this.hi_ < this.maxHi_) {
                ++this.hi_;
            } else {
                this.seek(this.curr_);
                if (this.curr_ == this.hi_) {
                    ++this.hi_;
                }
            }
        }
        
        this.buff_[(int)(this.curr_ - this.lo_)] = (byte)b;
        ++this.curr_;
        this.dirty_ = true;
    }
    
    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }
    
    public void write(byte[] b, int off, int len) throws IOException {
        while(len > 0) {
            int n = this.writeAtMost(b, off, len);
            off += n;
            len -= n;
            this.dirty_ = true;
        }
        
    }
    
    private int writeAtMost(byte[] b, int off, int len) throws IOException {
        if (this.curr_ >= this.hi_) {
            if (this.hitEOF_ && this.hi_ < this.maxHi_) {
                this.hi_ = this.maxHi_;
            } else {
                this.seek(this.curr_);
                if (this.curr_ == this.hi_) {
                    this.hi_ = this.maxHi_;
                }
            }
        }
        
        len = Math.min(len, (int)(this.hi_ - this.curr_));
        int buffOff = (int)(this.curr_ - this.lo_);
        System.arraycopy(b, off, this.buff_, buffOff, len);
        this.curr_ += (long)len;
        return len;
    }
    
    public String readInLine() throws IOException {
        StringBuilder input = new StringBuilder();
        int c = -1;
        boolean eol = false;
        
        while(!eol) {
            switch(c = this.read()) {
                case -1:
                case 10:
                    eol = true;
                    break;
                case 13:
                    eol = true;
                    long cur = this.getFilePointer();
                    if (this.read() != 10) {
                        this.seek(cur);
                    }
                    break;
                default:
                    input.append((char)c);
            }
        }
        
        if (c == -1 && input.length() == 0) {
            return null;
        } else {
            return input.toString();
        }
    }
}
