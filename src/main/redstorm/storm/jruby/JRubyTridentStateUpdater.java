package redstorm.storm.jruby;

import java.util.List;
import org.jruby.Ruby;
import org.jruby.runtime.Helpers;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.javasupport.JavaUtil;
import org.jruby.RubyModule;
import org.jruby.exceptions.RaiseException;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class JRubyTridentStateUpdater extends BaseStateUpdater<State> {
    private final String _realClassName;
    private final String _bootstrap;

    // transient to avoid serialization
    private transient IRubyObject _ruby_function;
    private transient Ruby __ruby__;

    public JRubyTridentStateUpdater(final String baseClassPath, final String realClassName) {
      _realClassName = realClassName;
      _bootstrap = "require '" + baseClassPath + "'";
    }

    @Override
    public void updateState(State s, List<TridentTuple> list, TridentCollector tc) {
        if(_ruby_function == null) {
          _ruby_function = initialize_ruby_function();
        }

        IRubyObject ruby_state = JavaUtil.convertJavaToRuby(__ruby__, s);
        IRubyObject ruby_list = JavaUtil.convertJavaToRuby(__ruby__, list);
        IRubyObject ruby_trident_colector = JavaUtil.convertJavaToRuby(__ruby__, tc);

        Helpers.invoke(__ruby__.getCurrentContext(), _ruby_function, "update_state", ruby_state, ruby_list, ruby_trident_colector);
    }

    private IRubyObject initialize_ruby_function() {
        __ruby__ = Ruby.getGlobalRuntime();

        RubyModule ruby_class;
        try {
            ruby_class = __ruby__.getClassFromPath(_realClassName);
        }
        catch (RaiseException e) {
            // after deserialization we need to recreate ruby environment
            __ruby__.evalScriptlet(_bootstrap);
            ruby_class = __ruby__.getClassFromPath(_realClassName);
        }
        return Helpers.invoke(__ruby__.getCurrentContext(), ruby_class, "new");
    }
}